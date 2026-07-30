#!/usr/bin/env node

import { readHookPayload } from "./lib/hook-payload.mjs";

const REMINDER = "Consider using GrepAI before running broad search commands.";

const payload = await readHookPayload();
const command = String(payload?.tool_input?.command ?? "");

const inspectionCommandPattern = /\b(rg|grep|find|cat|sed|nl|head|tail|less)\b/;

if (!inspectionCommandPattern.test(command)) {
  process.exit(0);
}

process.stdout.write(
  `${JSON.stringify({
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      additionalContext: REMINDER,
    },
  })}\n`,
);
