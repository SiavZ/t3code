import { useEffect, useState, type ReactNode } from "react";
import { Image, Linking, ScrollView, TextInput, View } from "react-native";
import { Markdown } from "react-native-nitro-markdown";
import type {
  AgentDocument,
  AgentDocumentAction,
  AgentDocumentActionInput,
  AgentDocumentNode,
} from "@t3tools/contracts";
import { safeDocumentUrl } from "@t3tools/client-runtime/agent-documents/model";
import { AppText } from "../../components/AppText";
import { ControlPill } from "../../components/ControlPill";
import { ThemedSwitch } from "../../components/ThemedSwitch";

export function AgentDocumentView(props: {
  readonly document: AgentDocument;
  readonly connected: boolean;
  readonly onAction: (
    action: AgentDocumentAction,
    state: AgentDocumentActionInput["state"],
  ) => Promise<void>;
  readonly onClose: () => Promise<void>;
  readonly assetUrl?: string;
}) {
  const { document } = props;
  const [state, setState] = useState<AgentDocumentActionInput["state"]>(
    document.body.kind === "applet" ? document.body.state : {},
  );
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    setState(document.body.kind === "applet" ? document.body.state : {});
  }, [document]);
  const disabled = pending || !props.connected || document.closed;
  const submit = async (action: AgentDocumentAction) => {
    if (disabled) return;
    setPending(true);
    setError(null);
    try {
      await props.onAction(action, state);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Document action failed.");
    } finally {
      setPending(false);
    }
  };
  const openPdf = async () => {
    if (disabled || !props.assetUrl) return;
    setPending(true);
    setError(null);
    try {
      await Linking.openURL(props.assetUrl);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "PDF viewer unavailable.");
    } finally {
      setPending(false);
    }
  };
  const close = async () => {
    if (pending || !props.connected) return;
    setPending(true);
    setError(null);
    try {
      await props.onClose();
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Document mutation failed.");
    } finally {
      setPending(false);
    }
  };
  const bind = (node: AgentDocumentNode, value: string | boolean) => {
    if (!disabled && node.bind) setState((previous) => ({ ...previous, [node.bind!]: value }));
  };
  const render = (node: AgentDocumentNode, key: string): ReactNode => {
    const label = node.label ?? node.title ?? node.text ?? node.type;
    const children = node.children?.map((child, index) => render(child, `${key}.${index}`));
    switch (node.type) {
      case "button":
      case "chip":
      case "list_item":
        return (
          <ControlPill
            key={key}
            label={label}
            accessibilityLabel={label}
            variant="pill"
            disabled={disabled || !node.on_press}
            onPress={() => node.on_press && void submit(node.on_press)}
          />
        );
      case "toggle":
        return (
          <View key={key} className="flex-row items-center gap-2">
            <AppText>{label}</AppText>
            <ThemedSwitch
              disabled={disabled}
              accessibilityLabel={label}
              value={Boolean(node.bind && state[node.bind])}
              onValueChange={(value) => bind(node, value)}
            />
          </View>
        );
      case "input":
        return (
          <View key={key}>
            <TextInput
              accessibilityLabel={label}
              editable={!disabled}
              multiline={node.multiline}
              placeholder={node.placeholder}
              value={String((node.bind && state[node.bind]) ?? "")}
              onChangeText={(value) => bind(node, value)}
              onSubmitEditing={() => node.on_submit && void submit(node.on_submit)}
              className="rounded-lg border border-border p-3 text-foreground"
            />
            {node.on_submit && (
              <ControlPill
                label="Submit"
                variant="pill"
                disabled={disabled}
                onPress={() => node.on_submit && void submit(node.on_submit)}
              />
            )}
          </View>
        );
      case "select":
        return (
          <View key={key} accessibilityLabel={label}>
            <AppText>{label}</AppText>
            <View className="flex-row flex-wrap gap-2">
              {node.options?.map((option) => (
                <ControlPill
                  key={option.value}
                  label={option.label}
                  variant={node.bind && state[node.bind] === option.value ? "primary" : "pill"}
                  disabled={disabled}
                  onPress={() => bind(node, option.value)}
                />
              ))}
            </View>
          </View>
        );
      case "tabs": {
        const selected =
          node.tabs?.find((tab) => tab.id === (node.bind && state[node.bind])) ?? node.tabs?.[0];
        return (
          <View key={key}>
            <View className="flex-row flex-wrap gap-2">
              {node.tabs?.map((tab) => (
                <ControlPill
                  key={tab.id}
                  label={tab.label}
                  variant={tab.id === selected?.id ? "primary" : "pill"}
                  disabled={disabled}
                  onPress={() => bind(node, tab.id)}
                />
              ))}
            </View>
            {selected?.children.map((child, index) => render(child, `${key}.tab.${index}`))}
          </View>
        );
      }
      case "markdown":
        return <Markdown key={key}>{node.text ?? ""}</Markdown>;
      case "image": {
        const url = node.source?.url && safeDocumentUrl(node.source.url);
        return url ? (
          <Image
            key={key}
            source={{ uri: url }}
            accessibilityLabel={node.alt ?? "Image"}
            style={{ width: "100%", height: 200 }}
            resizeMode="contain"
          />
        ) : (
          <AppText key={key}>Image unavailable.</AppText>
        );
      }
      case "table":
      case "key_value":
        return (
          <ScrollView key={key} horizontal>
            <View>
              {node.columns && <AppText>{node.columns.join(" | ")}</AppText>}
              {node.rows?.map((row, index) => (
                <AppText key={index}>{row.join(" | ")}</AppText>
              ))}
            </View>
          </ScrollView>
        );
      case "progress":
        return (
          <AppText
            key={key}
            accessibilityRole="progressbar"
            accessibilityValue={{ min: 0, max: 100, now: Math.round((node.value ?? 0) * 100) }}
          >
            {label}: {Math.round((node.value ?? 0) * 100)}%
          </AppText>
        );
      case "spacer":
        return <View key={key} className="h-2" />;
      case "divider":
        return <View key={key} className="border-t border-border" />;
      case "scroll":
        return (
          <ScrollView key={key} style={{ maxHeight: 384 }}>
            {children}
          </ScrollView>
        );
      case "stack":
      case "grid":
      case "card":
      case "list":
        return (
          <View
            key={key}
            className={node.direction === "horizontal" ? "flex-row flex-wrap gap-2" : "gap-2"}
          >
            {node.title && <AppText>{node.title}</AppText>}
            {children}
          </View>
        );
      default:
        return (
          <AppText key={key} accessibilityRole={node.type === "error" ? "alert" : undefined}>
            {label}
          </AppText>
        );
    }
  };
  return (
    <View accessibilityLabel={document.title} className="gap-3 rounded-lg border border-border p-3">
      <View className="flex-row items-center justify-between">
        <AppText>{document.title}</AppText>
        <ControlPill
          label={document.closed ? "Reopen" : "Close"}
          variant="pill"
          disabled={!props.connected || pending}
          onPress={() => void close()}
        />
      </View>
      {!props.connected && <AppText>Disconnected. Document controls are disabled.</AppText>}
      {error && <AppText accessibilityRole="alert">{error}</AppText>}
      {!document.closed &&
        (document.body.kind === "applet" ? (
          render(document.body.view, "root")
        ) : document.body.kind === "markdown" ? (
          <Markdown>{document.body.content}</Markdown>
        ) : props.assetUrl ? (
          <View>
            <AppText>PDF opens in a compatible viewer on this device.</AppText>
            <ControlPill
              label="Open PDF"
              variant="pill"
              disabled={disabled}
              onPress={() => void openPdf()}
            />
          </View>
        ) : (
          <AppText>PDF asset unavailable.</AppText>
        ))}
    </View>
  );
}
