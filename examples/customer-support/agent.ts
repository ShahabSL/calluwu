import { Agent, scriptedVoice } from "@calluwu/sdk";

export default new Agent({
  name: "customer-support",
  instructions: `
    You are a concise customer support agent.
    Confirm identity before discussing account-specific information.
    Never invent an account state or claim a tool succeeded when it failed.
  `,
  // The checked-in fixture is intentionally credential-free. Production agents use
  // cloudflareVoice(); unsupported vendors are never emulated.
  ...scriptedVoice(),
});
