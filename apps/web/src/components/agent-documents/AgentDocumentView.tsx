import { useEffect, useState } from "react";
import type {
  AgentDocument,
  AgentDocumentAction,
  AgentDocumentActionInput,
  AgentDocumentNode,
} from "@t3tools/contracts";
import { safeDocumentUrl } from "@t3tools/client-runtime/agent-documents/model";
import ChatMarkdown from "../ChatMarkdown";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Textarea } from "../ui/textarea";
import { Switch } from "../ui/switch";

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
  const render = (node: AgentDocumentNode, key: string): React.ReactNode => {
    const label = node.label ?? node.title ?? node.text ?? node.type;
    const children = node.children?.map((child, index) => render(child, `${key}.${index}`));
    switch (node.type) {
      case "button":
      case "chip":
      case "list_item":
        return (
          <Button
            key={key}
            disabled={disabled || !node.on_press}
            variant="outline"
            onClick={() => node.on_press && void submit(node.on_press)}
          >
            {label}
          </Button>
        );
      case "toggle":
        return (
          <label key={key} className="flex items-center gap-2">
            <Switch
              disabled={disabled}
              checked={Boolean(node.bind && state[node.bind])}
              onCheckedChange={(value) => bind(node, value)}
            />
            {label}
          </label>
        );
      case "input": {
        const input = {
          disabled,
          "aria-label": label,
          placeholder: node.placeholder,
          value: String((node.bind && state[node.bind]) ?? ""),
          onChange: (event: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) =>
            bind(node, event.target.value),
        };
        return (
          <form
            key={key}
            onSubmit={(event) => {
              event.preventDefault();
              if (node.on_submit) void submit(node.on_submit);
            }}
          >
            {node.multiline ? <Textarea {...input} /> : <Input {...input} />}
            {node.on_submit && (
              <Button disabled={disabled} type="submit">
                Submit
              </Button>
            )}
          </form>
        );
      }
      case "select":
        return (
          <fieldset key={key} disabled={disabled}>
            <legend>{label}</legend>
            <div className="flex flex-wrap gap-2">
              {node.options?.map((option) => (
                <Button
                  key={option.value}
                  variant={node.bind && state[node.bind] === option.value ? "default" : "outline"}
                  aria-pressed={node.bind ? state[node.bind] === option.value : false}
                  onClick={() => bind(node, option.value)}
                >
                  {option.label}
                </Button>
              ))}
            </div>
          </fieldset>
        );
      case "tabs": {
        const selected =
          node.tabs?.find((tab) => tab.id === (node.bind && state[node.bind])) ?? node.tabs?.[0];
        return (
          <div key={key}>
            <div role="tablist" aria-label={label}>
              {node.tabs?.map((tab) => (
                <Button
                  key={tab.id}
                  role="tab"
                  aria-selected={tab.id === selected?.id}
                  disabled={disabled}
                  variant="ghost"
                  onClick={() => bind(node, tab.id)}
                >
                  {tab.label}
                </Button>
              ))}
            </div>
            <div role="tabpanel">
              {selected?.children.map((child, index) => render(child, `${key}.tab.${index}`))}
            </div>
          </div>
        );
      }
      case "markdown":
        return <ChatMarkdown key={key} text={node.text ?? ""} cwd={undefined} />;
      case "code":
        return (
          <pre key={key} className="overflow-auto whitespace-pre-wrap">
            <code>{node.text}</code>
          </pre>
        );
      case "image": {
        const url = node.source?.url && safeDocumentUrl(node.source.url);
        return url ? (
          <img
            key={key}
            src={url}
            alt={node.alt ?? ""}
            loading="lazy"
            referrerPolicy="no-referrer"
          />
        ) : (
          <p key={key}>Image unavailable.</p>
        );
      }
      case "table":
      case "key_value":
        return (
          <div key={key} className="overflow-auto">
            <table>
              <thead>
                <tr>
                  {node.columns?.map((column, index) => (
                    <th key={index} scope="col">
                      {column}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {node.rows?.map((row, index) => (
                  <tr key={index}>
                    {row.map((cell, cellIndex) => (
                      <td key={cellIndex}>{cell}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        );
      case "progress":
        return (
          <label key={key}>
            {label}
            <progress value={node.value ?? 0} max={1} />
          </label>
        );
      case "divider":
        return <hr key={key} />;
      case "spacer":
        return <div key={key} className="h-2" />;
      case "error":
        return (
          <p key={key} role="alert">
            {label}
          </p>
        );
      case "text":
      case "empty":
      case "icon":
        return <p key={key}>{label}</p>;
      case "scroll":
        return (
          <div key={key} className="max-h-96 overflow-auto">
            {children}
          </div>
        );
      case "card":
        return (
          <section key={key} className="rounded-lg border p-3">
            <h3>{node.title}</h3>
            {children}
          </section>
        );
      default:
        return (
          <div
            key={key}
            className={
              node.direction === "horizontal" ? "flex flex-wrap gap-2" : "flex flex-col gap-2"
            }
          >
            {children}
          </div>
        );
    }
  };
  return (
    <section aria-label={document.title} className="rounded-lg border p-3">
      <header className="flex items-center justify-between gap-2">
        <h2>{document.title}</h2>
        <Button variant="ghost" disabled={!props.connected || pending} onClick={() => void close()}>
          {document.closed ? "Reopen" : "Close"}
        </Button>
      </header>
      {!props.connected && <p role="status">Disconnected. Document controls are disabled.</p>}
      {error && <p role="alert">{error}</p>}
      {!document.closed &&
        (document.body.kind === "applet" ? (
          render(document.body.view, "root")
        ) : document.body.kind === "markdown" ? (
          <ChatMarkdown text={document.body.content} cwd={undefined} />
        ) : props.assetUrl ? (
          <iframe title={document.title} src={props.assetUrl} className="h-96 w-full" />
        ) : (
          <p>PDF asset unavailable.</p>
        ))}
    </section>
  );
}
