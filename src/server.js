import { createPlatform } from "./app.js";

const port = Number(process.env.PORT ?? 0);
const host = process.env.HOST ?? "127.0.0.1";

const platform = createPlatform({ seedDemo: true, startBackground: true });

platform.server.listen(port, host, () => {
  const addr = platform.server.address();
  const boundPort = typeof addr === "object" && addr ? addr.port : port;
  const tenantId = platform.demoTenant?.id ?? "none";
  console.log(`Firm Data Copilot MVP listening on http://${host}:${boundPort}`);
  console.log(`Demo tenant id: ${tenantId}`);
});

function shutdown() {
  platform.close();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
