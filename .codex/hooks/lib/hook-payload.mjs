export async function readHookPayload() {
  try {
    const chunks = [];

    for await (const chunk of process.stdin) {
      chunks.push(chunk);
    }

    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    return {};
  }
}
