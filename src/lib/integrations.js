const INTEGRATION_CATALOG = [
  {
    key: "google_ads",
    name: "Google Ads",
    kind: "source",
    sourceType: "google_ads",
    mode: "hybrid",
    initialDomain: "marketing",
    description: "Sync campaign spend, leads, and performance metrics."
  },
  {
    key: "meta_ads",
    name: "Meta Ads",
    kind: "source",
    sourceType: "meta_ads",
    mode: "hybrid",
    initialDomain: "marketing",
    description: "Sync Meta campaign spend, leads, and attribution signals."
  },
  {
    key: "hubspot",
    name: "HubSpot",
    kind: "source",
    sourceType: "hubspot",
    mode: "hybrid",
    initialDomain: "sales",
    description: "Sync CRM, funnel, and lifecycle stages."
  },
  {
    key: "salesforce",
    name: "Salesforce",
    kind: "source",
    sourceType: "salesforce",
    mode: "hybrid",
    initialDomain: "sales",
    description: "Sync opportunities, pipeline, and account activity."
  },
  {
    key: "quickbooks",
    name: "QuickBooks",
    kind: "source",
    sourceType: "quickbooks",
    mode: "hybrid",
    initialDomain: "finance",
    description: "Sync accounting and financial ledger data."
  },
  {
    key: "stripe",
    name: "Stripe",
    kind: "source",
    sourceType: "stripe",
    mode: "hybrid",
    initialDomain: "finance",
    description: "Sync billing, subscription, and cash-in metrics."
  },
  {
    key: "postgres",
    name: "PostgreSQL",
    kind: "source",
    sourceType: "postgres",
    mode: "hybrid",
    initialDomain: "ops",
    description: "Connect product and operational data from Postgres."
  },
  {
    key: "snowflake",
    name: "Snowflake",
    kind: "source",
    sourceType: "snowflake",
    mode: "hybrid",
    initialDomain: "ops",
    description: "Use warehouse-native analytics data."
  },
  {
    key: "google_sheets",
    name: "Google Sheets",
    kind: "source",
    sourceType: "google_sheets",
    mode: "ingest",
    initialDomain: "ops",
    description: "Ingest planning and spreadsheet datasets."
  },
  {
    key: "excel_365",
    name: "Excel 365",
    kind: "source",
    sourceType: "excel_365",
    mode: "ingest",
    initialDomain: "ops",
    description: "Ingest workbook-based datasets from Microsoft 365."
  },
  {
    key: "slack_channel",
    name: "Slack Channel",
    kind: "channel",
    channel: "slack",
    description: "Enable Slack delivery and notifications."
  },
  {
    key: "telegram_alerts",
    name: "Telegram Alerts",
    kind: "channel",
    channel: "telegram",
    description: "Enable Telegram alerts and run summaries."
  },
  {
    key: "google_drive_mcp",
    name: "Google Drive MCP",
    kind: "mcp",
    provider: "google-drive",
    defaultEndpoint: "https://mcp.example.com/google-drive",
    description: "Expose Drive docs/sheets resources to agents."
  },
  {
    key: "filesystem_mcp",
    name: "Filesystem MCP",
    kind: "mcp",
    provider: "filesystem",
    defaultEndpoint: "",
    description: "Let agents work on approved workspace folders."
  }
];

function findIntegration(key) {
  return INTEGRATION_CATALOG.find((item) => item.key === key);
}

export function listIntegrationsCatalog() {
  return INTEGRATION_CATALOG;
}

function requireIntegration(key) {
  const integration = findIntegration(key);
  if (!integration) {
    const err = new Error(`Integration '${key}' is not supported`);
    err.statusCode = 404;
    throw err;
  }
  return integration;
}

export function quickAddIntegration(state, tenant, payload = {}, adapters = {}) {
  const integration = requireIntegration(payload.integrationKey);
  const result = {
    integrationKey: integration.key,
    kind: integration.kind,
    createdAt: new Date().toISOString()
  };

  if (integration.kind === "source") {
    if (!adapters.createSourceConnection) {
      const err = new Error("Source adapter unavailable");
      err.statusCode = 500;
      throw err;
    }
    const auth = payload.auth ?? (payload.authRef ? { token: payload.authRef } : {});
    const connection = adapters.createSourceConnection(state, tenant, {
      sourceType: integration.sourceType,
      mode: payload.mode ?? integration.mode ?? "hybrid",
      auth,
      syncPolicy: {
        intervalMinutes: Number(payload.intervalMinutes ?? 60),
        backfillDays: Number(payload.backfillDays ?? 30),
        freshnessSlaHours: Number(payload.freshnessSlaHours ?? 24)
      },
      metadata: {
        owner: payload.owner ?? "integrations-quick-add",
        qualityChecks: payload.qualityChecks ?? ["null_check", "duplicate_guard", "spike_check"]
      }
    });

    result.connection = {
      id: connection.id,
      sourceType: connection.sourceType,
      mode: connection.mode,
      status: connection.status
    };

    const shouldSync = payload.runInitialSync !== false && connection.mode !== "live";
    if (shouldSync && adapters.runSourceSync) {
      const sync = adapters.runSourceSync(state, tenant, connection, {
        domain: payload.domain ?? integration.initialDomain,
        periodDays: Number(payload.periodDays ?? 14)
      });
      result.initialSync = {
        sourceRunId: sync.sourceRunId,
        syncStatus: sync.syncStatus
      };
    }
    return result;
  }

  if (integration.kind === "channel") {
    if (!adapters.patchSettingsChannels) {
      const err = new Error("Channel adapter unavailable");
      err.statusCode = 500;
      throw err;
    }
    if (integration.channel === "slack") {
      if (!payload.webhookRef) {
        const err = new Error("webhookRef is required for Slack quick add");
        err.statusCode = 400;
        throw err;
      }
      adapters.patchSettingsChannels(state, tenant, {
        slack: {
          enabled: true,
          webhookRef: payload.webhookRef
        }
      });
      result.channel = "slack";
      result.enabled = true;
      return result;
    }
    if (integration.channel === "telegram") {
      if (!payload.botTokenRef) {
        const err = new Error("botTokenRef is required for Telegram quick add");
        err.statusCode = 400;
        throw err;
      }
      adapters.patchSettingsChannels(state, tenant, {
        telegram: {
          enabled: true,
          botTokenRef: payload.botTokenRef,
          chatId: payload.chatId ?? ""
        }
      });
      result.channel = "telegram";
      result.enabled = true;
      return result;
    }
  }

  if (integration.kind === "mcp") {
    if (!adapters.createMcpServer) {
      const err = new Error("MCP adapter unavailable");
      err.statusCode = 500;
      throw err;
    }
    const server = adapters.createMcpServer(state, tenant, {
      provider: integration.provider,
      name: payload.name ?? integration.name,
      endpoint: payload.endpoint ?? integration.defaultEndpoint ?? "",
      authRef: payload.authRef ?? "",
      allowedFolderIds: payload.allowedFolderIds ?? [],
      capabilities: payload.capabilities ?? ["resources.list", "resources.read"]
    });
    result.server = {
      id: server.id,
      provider: server.provider,
      status: server.status
    };
    if (payload.testAfterCreate !== false && adapters.testMcpServer) {
      result.test = adapters.testMcpServer(state, tenant.id, server.id);
    }
    return result;
  }

  const err = new Error(`Integration kind '${integration.kind}' is not implemented`);
  err.statusCode = 400;
  throw err;
}
