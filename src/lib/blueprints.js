export const BLUEPRINTS = {
  "starter-marketing": {
    id: "starter-marketing",
    name: "Starter Marketing",
    domains: ["marketing"],
    metrics: [
      { id: "spend", formula: "sum(spend)", grain: "day", domain: "marketing" },
      { id: "leads", formula: "sum(leads)", grain: "day", domain: "marketing" },
      { id: "revenue", formula: "sum(revenue)", grain: "day", domain: "marketing" },
      { id: "roas", formula: "revenue / spend", grain: "day", domain: "marketing" }
    ]
  },
  "starter-finance": {
    id: "starter-finance",
    name: "Starter Finance",
    domains: ["finance"],
    metrics: [
      { id: "cash_in", formula: "sum(cash_in)", grain: "day", domain: "finance" },
      { id: "cash_out", formula: "sum(cash_out)", grain: "day", domain: "finance" },
      { id: "profit", formula: "cash_in - cash_out", grain: "day", domain: "finance" },
      { id: "runway_days", formula: "cash_balance / avg_daily_burn", grain: "day", domain: "finance" }
    ]
  },
  "cross-domain": {
    id: "cross-domain",
    name: "Cross-Domain",
    domains: ["marketing", "finance", "sales", "ops"],
    metrics: [
      { id: "spend", formula: "sum(spend)", grain: "day", domain: "marketing" },
      { id: "revenue", formula: "sum(revenue)", grain: "day", domain: "marketing" },
      { id: "leads", formula: "sum(leads)", grain: "day", domain: "marketing" },
      { id: "profit", formula: "cash_in - cash_out", grain: "day", domain: "finance" },
      { id: "cash_in", formula: "sum(cash_in)", grain: "day", domain: "finance" },
      { id: "cash_out", formula: "sum(cash_out)", grain: "day", domain: "finance" }
    ]
  }
};

export function getBlueprint(blueprintId = "cross-domain") {
  return BLUEPRINTS[blueprintId] ?? BLUEPRINTS["cross-domain"];
}

export function listBlueprints() {
  return Object.values(BLUEPRINTS).map((item) => ({
    id: item.id,
    name: item.name,
    domains: item.domains
  }));
}
