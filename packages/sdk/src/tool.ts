import type { JsonValue, ToolDefinition } from "@calluwu/types";
import { ToolDefinitionSchema } from "@calluwu/types";

export type ToolContext = {
  organizationId?: string;
  projectId?: string;
  sessionId: string;
  toolCallId: string;
  idempotencyKey: string;
  signal: AbortSignal;
};

export type ToolHandler<
  Input extends JsonValue = JsonValue,
  Output extends JsonValue = JsonValue,
> = (input: Input, context: ToolContext) => Promise<Output> | Output;

const TOOL_MARKER = Symbol.for("calluwu.tool");

export class Tool<Input extends JsonValue = JsonValue, Output extends JsonValue = JsonValue> {
  readonly [TOOL_MARKER] = true;
  readonly definition: ToolDefinition;
  readonly handler?: ToolHandler<Input, Output>;

  constructor(definition: ToolDefinition, handler?: ToolHandler<Input, Output>) {
    this.definition = ToolDefinitionSchema.parse(definition);
    if (handler !== undefined) {
      this.handler = handler;
    }
  }
}

export function defineTool<
  Input extends JsonValue = JsonValue,
  Output extends JsonValue = JsonValue,
>(
  definition: Omit<ToolDefinition, "execution">,
  handler: ToolHandler<Input, Output>,
): Tool<Input, Output> {
  return new Tool({ ...definition, execution: { kind: "local" } }, handler);
}

export function httpTool(
  definition: Omit<ToolDefinition, "execution"> & {
    url: string;
    secretRef?: string;
  },
): Tool {
  const { url, secretRef, ...tool } = definition;
  const execution = secretRef
    ? { kind: "https" as const, url, secretRef }
    : { kind: "https" as const, url };
  return new Tool({ ...tool, execution });
}

export function builtinTool(
  definition: Omit<ToolDefinition, "execution"> & { integration: string },
): Tool {
  const { integration, ...tool } = definition;
  return new Tool({ ...tool, execution: { kind: "builtin", integration } });
}

export function isTool(value: unknown): value is Tool {
  return (
    typeof value === "object" &&
    value !== null &&
    TOOL_MARKER in value &&
    (value as { [TOOL_MARKER]?: unknown })[TOOL_MARKER] === true
  );
}
