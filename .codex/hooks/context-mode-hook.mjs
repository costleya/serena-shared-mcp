#!/usr/bin/env node

import { spawn } from "node:child_process";

async function readStdin() {
  const chunks = [];
  for await (const chunk of process.stdin) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

async function forwardToContextMode(input) {
  const child = spawn("context-mode", ["hook", "codex", ...process.argv.slice(2)], {
    stdio: ["pipe", "inherit", "inherit"],
  });
  const signals = ["SIGHUP", "SIGINT", "SIGQUIT", "SIGTERM"];
  const signalHandlers = new Map();
  let childClosed = false;
  let receivedSignal;
  let launchError;

  for (const signal of signals) {
    const handler = () => {
      if (receivedSignal === undefined) {
        receivedSignal = signal;
      }
      if (!childClosed) {
        child.kill(signal);
      }
    };
    signalHandlers.set(signal, handler);
    process.once(signal, handler);
  }

  child.stdin.on("error", () => {});
  const resultPromise = new Promise((resolve) => {
    child.once("error", (error) => {
      launchError = error;
    });
    child.once("close", (code, signal) => {
      childClosed = true;
      resolve({ code, signal });
    });
  });
  child.stdin.end(input);
  const result = await resultPromise;

  for (const [signal, handler] of signalHandlers) {
    process.removeListener(signal, handler);
  }

  if (receivedSignal !== undefined) {
    process.kill(process.pid, receivedSignal);
    return;
  }
  if (launchError !== undefined) {
    const code = typeof launchError.code === "string" ? ` (${launchError.code})` : "";
    process.stderr.write(`context-mode hook launch failed${code}: ${launchError.message}\n`);
    process.exitCode = 1;
    return;
  }
  if (result.signal) {
    process.kill(process.pid, result.signal);
    return;
  }
  process.exitCode = result.code ?? 1;
}

const input = await readStdin();
let payload;
try {
  payload = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(input));
} catch {
  payload = undefined;
}

if (typeof payload?.agent_id === "string" && payload.agent_id) {
  process.exit(0);
}

await forwardToContextMode(input);
