import type { AgentDefinition, ToolDefinition } from "@calluwu/types";
import { AgentDefinitionSchema } from "@calluwu/types";
import { cloudflareVoice } from "./presets.js";
import { isTool, type Tool } from "./tool.js";

const AGENT_MARKER = Symbol.for("calluwu.agent");

export type AgentOptions = Omit<
  AgentDefinition,
  "providers" | "voice" | "tools" | "limits" | "metadata"
> & {
  providers?: AgentDefinition["providers"];
  voice?: Partial<AgentDefinition["voice"]> & Pick<AgentDefinition["voice"], "id">;
  tools?: Tool[];
  limits?: Partial<AgentDefinition["limits"]>;
  metadata?: AgentDefinition["metadata"];
};

const defaultVoice = cloudflareVoice();

export class Agent {
  readonly [AGENT_MARKER] = true;
  readonly definition: AgentDefinition;
  readonly tools: readonly Tool[];

  constructor(options: AgentOptions) {
    const tools = options.tools ?? [];
    for (const tool of tools) {
      if (!isTool(tool)) {
        throw new TypeError(
          "Agent tools must be created with defineTool, httpTool, or builtinTool",
        );
      }
    }

    const toolDefinitions: ToolDefinition[] = tools.map((tool) => tool.definition);
    this.definition = AgentDefinitionSchema.parse({
      name: options.name,
      instructions: options.instructions,
      providers: options.providers ?? defaultVoice.providers,
      voice: {
        id: options.voice?.id ?? defaultVoice.voice.id,
        language: options.voice?.language ?? defaultVoice.voice.language,
        sampleRateHz: options.voice?.sampleRateHz ?? defaultVoice.voice.sampleRateHz,
      },
      tools: toolDefinitions,
      limits: options.limits ?? {},
      metadata: options.metadata ?? {},
    });
    this.tools = Object.freeze([...tools]);
  }

  toJSON(): AgentDefinition {
    return structuredClone(this.definition);
  }
}

export function isAgent(value: unknown): value is Agent {
  return (
    typeof value === "object" &&
    value !== null &&
    AGENT_MARKER in value &&
    (value as { [AGENT_MARKER]?: unknown })[AGENT_MARKER] === true
  );
}
