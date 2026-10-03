import { ProjectId, ProviderInstanceId } from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderRegistry from "./Services/ProviderRegistry.ts";
import * as SkillManagement from "./SkillManagement.ts";

it.layer(NodeServices.layer)("SkillManagement", (it) => {
  it.effect(
    "loads workspace overlay content and refreshes inventory without rewriting skills",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const cwd = yield* fs.makeTempDirectoryScoped();
        const path = `${cwd}/SKILL.md`;
        yield* fs.writeFileString(path, "# Fixture skill\nDo fixture work.");
        const instanceId = ProviderInstanceId.make("fixture");
        const projectId = ProjectId.make("fixture");
        const requests: boolean[] = [];
        const provider = {
          instanceId,
          skills: [],
          workspaceSnapshots: [
            {
              cwd,
              checkedAt: "2026-10-03T00:00:00.000Z",
              slashCommands: [],
              skills: [{ name: "fixture", path, enabled: true }],
            },
          ],
        };
        const registry = Layer.succeed(ProviderRegistry.ProviderRegistry, {
          getProviders: Effect.succeed([provider]),
          refresh: () => Effect.succeed([provider]),
          refreshInstance: () => Effect.succeed([provider]),
          refreshWorkspaceSnapshot: (
            input: Parameters<
              ProviderRegistry.ProviderRegistryShape["refreshWorkspaceSnapshot"]
            >[0],
          ) => {
            requests.push(input.fresh ?? false);
            return Effect.succeed([provider]);
          },
          getProviderMaintenanceCapabilitiesForInstance: () => Effect.die("unused"),
          setProviderMaintenanceActionState: () => Effect.succeed([provider]),
          streamChanges: Stream.empty,
        } as unknown as ProviderRegistry.ProviderRegistryShape);
        const snapshots = Layer.succeed(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
          getProjectShellById: () => Effect.succeed(Option.some({ workspaceRoot: cwd })),
        } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQueryShape);
        yield* Effect.gen(function* () {
          const service = yield* SkillManagement.SkillManagement;
          expect(
            (yield* service.load({ projectId, instanceId, name: "fixture" })).content,
          ).toContain("Do fixture work.");
          yield* service.reload({ projectId, instanceId });
          expect(requests).toEqual([false, true]);
          expect(yield* fs.readFileString(path)).toBe("# Fixture skill\nDo fixture work.");
          const error = yield* service
            .read({ projectId, instanceId, name: "missing" })
            .pipe(Effect.flip);
          expect(error.reason).toBe("skill-not-found");
        }).pipe(
          Effect.provide(
            SkillManagement.layer.pipe(Layer.provide(registry), Layer.provide(snapshots)),
          ),
        );
      }),
  );
});
