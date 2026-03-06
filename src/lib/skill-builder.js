const SUPPORTED_CHANNELS = new Set(["web", "slack", "telegram", "discord", "api", "email"]);
const SUPPORTED_TOOLS = new Set([
  "model.run",
  "reports.generate",
  "sources.sync",
  "notify.owner",
  "compute.data_quality_snapshot",
  "compute.finance_snapshot",
  "compute.deal_desk_snapshot"
]);

function asString(value) {
  return String(value ?? "").trim();
}

function parseCsv(value = "") {
  return String(value ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function semver(value) {
  const candidate = asString(value);
  if (/^\d+\.\d+\.\d+(-[a-z0-9.-]+)?$/i.test(candidate)) return candidate;
  return "1.0.0";
}

function slugifySkillId(name = "") {
  const slug = String(name ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 80);
  return slug || "custom-skill";
}

export function normalizeProviderName(value = "") {
  const raw = asString(value).toLowerCase();
  if (!raw || raw === "managed") return "managed";
  if (raw.includes("openai") || raw.includes("gpt")) return "openai";
  if (raw.includes("anthropic") || raw.includes("claude")) return "anthropic";
  if (raw.includes("gemini") || raw.includes("google")) return "gemini";
  return raw;
}

export function parseProviderCredentialEntry(entry = "") {
  const raw = asString(entry);
  if (!raw) return null;
  const separatorIndex = raw.includes("=") ? raw.indexOf("=") : raw.indexOf(":");
  if (separatorIndex < 0) {
    return { provider: normalizeProviderName(raw), token: "" };
  }
  const provider = normalizeProviderName(raw.slice(0, separatorIndex));
  const token = asString(raw.slice(separatorIndex + 1));
  return { provider, token };
}

export function maskApiKey(value = "") {
  const raw = asString(value);
  if (!raw) return "";
  if (raw.length <= 8) return `${raw.slice(0, 1)}***${raw.slice(-1)}`;
  return `${raw.slice(0, 4)}…${raw.slice(-4)}`;
}

export function buildSkillManifestFromAnswers(answers = {}) {
  const name = asString(answers.name) || "Custom Skill";
  const description = asString(answers.description) || "Tenant custom orchestration skill";
  const intents = parseCsv(answers.intents)
    .map((value) => value.toLowerCase().replace(/[^a-z0-9_:-]+/g, "_"))
    .filter(Boolean);
  const channels = parseCsv(answers.channels)
    .map((value) => value.toLowerCase())
    .filter((value) => SUPPORTED_CHANNELS.has(value));
  const tools = parseCsv(answers.tools).map((toolId) => {
    const id = asString(toolId);
    if (!id) return null;
    if (SUPPORTED_TOOLS.has(id) || id.startsWith("custom.")) {
      return { id, allow: true };
    }
    return { id: `custom.${slugifySkillId(id)}`, allow: true };
  }).filter(Boolean);
  const confidenceMin = Number(answers.confidenceMin ?? 0.7);
  const budgetCapUsd = Number(answers.budgetCapUsd ?? 10000);
  const systemPrompt = asString(answers.systemPrompt) || "Use available tenant data to generate actionable outcomes.";
  return {
    id: slugifySkillId(name),
    version: semver(answers.version || "1.0.0"),
    name,
    description,
    triggers: {
      intents: intents.length ? intents : ["custom_intent"],
      channels: channels.length ? channels : ["web", "api"]
    },
    tools: tools.length ? tools : [{ id: "model.run", allow: true }],
    guardrails: {
      confidenceMin: Math.max(0, Math.min(1, Number.isFinite(confidenceMin) ? confidenceMin : 0.7)),
      humanApprovalFor: parseCsv(answers.humanApprovalFor),
      budgetCapUsd: Number.isFinite(budgetCapUsd) ? Math.max(0, budgetCapUsd) : 10000,
      tokenBudget: 2500,
      timeBudgetMs: 8000,
      contextTokenBudget: 1400,
      killSwitch: false
    },
    prompts: {
      system: systemPrompt
    },
    schedules: []
  };
}

export function buildSkillMarkdown(manifest = {}) {
  const tools = Array.isArray(manifest.tools) ? manifest.tools : [];
  const lines = [
    `# ${manifest.name || "Custom Skill"}`,
    "",
    `id: ${manifest.id || "custom-skill"}`,
    `version: ${manifest.version || "1.0.0"}`,
    "",
    "## Description",
    manifest.description || "",
    "",
    "## Triggers",
    `intents: ${(manifest.triggers?.intents || []).join(", ")}`,
    `channels: ${(manifest.triggers?.channels || []).join(", ")}`,
    "",
    "## Tools",
    ...tools.map((tool) => `- ${tool.id} (allow=${tool.allow ? "true" : "false"})`),
    "",
    "## Guardrails",
    `confidenceMin: ${manifest.guardrails?.confidenceMin ?? 0.7}`,
    `budgetCapUsd: ${manifest.guardrails?.budgetCapUsd ?? 10000}`,
    `humanApprovalFor: ${(manifest.guardrails?.humanApprovalFor || []).join(", ")}`,
    "",
    "## Prompts",
    "```txt",
    manifest.prompts?.system || "",
    "```"
  ];
  return lines.join("\n");
}

function parseJsonFromModelText(text = "") {
  const raw = String(text ?? "").trim();
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {}
  const fenced = raw.match(/```json\s*([\s\S]*?)```/i) || raw.match(/```\s*([\s\S]*?)```/i);
  if (fenced?.[1]) {
    try {
      return JSON.parse(fenced[1]);
    } catch {}
  }
  const fromBrace = raw.indexOf("{");
  const toBrace = raw.lastIndexOf("}");
  if (fromBrace >= 0 && toBrace > fromBrace) {
    try {
      return JSON.parse(raw.slice(fromBrace, toBrace + 1));
    } catch {}
  }
  return null;
}

function sanitizeManifest(candidate, fallback) {
  const draft = {
    ...fallback,
    ...(candidate && typeof candidate === "object" ? candidate : {})
  };
  draft.id = slugifySkillId(draft.id || draft.name || fallback.id);
  draft.version = semver(draft.version || fallback.version);
  draft.name = asString(draft.name) || fallback.name;
  draft.description = asString(draft.description) || fallback.description;
  const intents = Array.isArray(draft.triggers?.intents) ? draft.triggers.intents.map((item) => asString(item).toLowerCase()).filter(Boolean) : [];
  const channels = Array.isArray(draft.triggers?.channels) ? draft.triggers.channels.map((item) => asString(item).toLowerCase()).filter((item) => SUPPORTED_CHANNELS.has(item)) : [];
  draft.triggers = {
    intents: intents.length ? intents : fallback.triggers.intents,
    channels: channels.length ? channels : fallback.triggers.channels
  };

  const tools = Array.isArray(draft.tools) ? draft.tools : [];
  const normalizedTools = tools
    .map((tool) => {
      const id = asString(tool?.id);
      if (!id) return null;
      if (!SUPPORTED_TOOLS.has(id) && !id.startsWith("custom.")) return null;
      return { id, allow: tool?.allow !== false };
    })
    .filter(Boolean);
  draft.tools = normalizedTools.length ? normalizedTools : fallback.tools;

  draft.guardrails = {
    ...fallback.guardrails,
    ...(draft.guardrails && typeof draft.guardrails === "object" ? draft.guardrails : {})
  };
  draft.guardrails.confidenceMin = Math.max(0, Math.min(1, Number(draft.guardrails.confidenceMin ?? fallback.guardrails.confidenceMin)));
  draft.guardrails.budgetCapUsd = Math.max(0, Number(draft.guardrails.budgetCapUsd ?? fallback.guardrails.budgetCapUsd));
  draft.guardrails.tokenBudget = Math.max(200, Number(draft.guardrails.tokenBudget ?? fallback.guardrails.tokenBudget));
  draft.guardrails.timeBudgetMs = Math.max(500, Number(draft.guardrails.timeBudgetMs ?? fallback.guardrails.timeBudgetMs));
  draft.guardrails.contextTokenBudget = Math.max(200, Number(draft.guardrails.contextTokenBudget ?? fallback.guardrails.contextTokenBudget));
  draft.guardrails.humanApprovalFor = Array.isArray(draft.guardrails.humanApprovalFor)
    ? draft.guardrails.humanApprovalFor.map((item) => asString(item)).filter(Boolean)
    : fallback.guardrails.humanApprovalFor;
  draft.guardrails.killSwitch = Boolean(draft.guardrails.killSwitch);

  const prompts = draft.prompts && typeof draft.prompts === "object" ? draft.prompts : {};
  draft.prompts = {
    system: asString(prompts.system) || fallback.prompts.system
  };
  draft.schedules = Array.isArray(draft.schedules) ? draft.schedules.filter((item) => item && typeof item === "object") : [];
  return draft;
}

function buildLlmPrompt(answers, workspaceAgentName) {
  return [
    "Create a strict JSON object for a tenant skill pack.",
    "Return ONLY valid JSON with shape:",
    "{",
    '  "manifest": { ... },',
    '  "skillMarkdown": "..."',
    "}",
    "",
    "Manifest requirements:",
    "- id uses lowercase letters, numbers, and hyphens only.",
    "- version must be semver.",
    "- include triggers, tools, guardrails, prompts, schedules.",
    "- tools should prefer model.run, reports.generate, compute.* deterministic tools.",
    "",
    `Workspace agent name: ${workspaceAgentName}`,
    `Builder answers JSON: ${JSON.stringify(answers)}`
  ].join("\n");
}

async function callOpenAi({
  apiKey,
  prompt,
  timeoutMs,
  systemPrompt = "You generate safe, policy-aware skill manifests for enterprise agents.",
  responseFormat = null,
  model = "gpt-4o-mini"
}) {
  const body = {
    model,
    temperature: 0.2,
    messages: [
      { role: "system", content: systemPrompt },
      { role: "user", content: prompt }
    ]
  };
  if (responseFormat && typeof responseFormat === "object") {
    body.response_format = responseFormat;
  }
  const response = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${apiKey}`
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(timeoutMs)
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = payload?.error?.message || `OpenAI request failed (${response.status})`;
    const error = new Error(message);
    error.statusCode = 502;
    throw error;
  }
  return {
    model: payload.model || model,
    text: payload.choices?.[0]?.message?.content || ""
  };
}

async function callAnthropic({
  apiKey,
  prompt,
  timeoutMs,
  systemPrompt = "You generate safe, policy-aware skill manifests for enterprise agents.",
  model = "claude-3-5-sonnet-latest"
}) {
  const response = await fetch("https://api.anthropic.com/v1/messages", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      "x-api-key": apiKey,
      "anthropic-version": "2023-06-01"
    },
    body: JSON.stringify({
      model,
      max_tokens: 1400,
      temperature: 0.2,
      system: systemPrompt,
      messages: [{ role: "user", content: prompt }]
    }),
    signal: AbortSignal.timeout(timeoutMs)
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = payload?.error?.message || `Anthropic request failed (${response.status})`;
    const error = new Error(message);
    error.statusCode = 502;
    throw error;
  }
  const text = Array.isArray(payload.content)
    ? payload.content.map((item) => item?.text || "").join("\n")
    : "";
  return {
    model: payload.model || model,
    text
  };
}

async function callGemini({
  apiKey,
  prompt,
  timeoutMs,
  systemPrompt = "You generate safe, policy-aware skill manifests for enterprise agents.",
  model = "gemini-1.5-pro",
  responseMimeType = null
}) {
  const endpoint = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(model)}:generateContent?key=${encodeURIComponent(apiKey)}`;
  const fullPrompt = systemPrompt ? `${systemPrompt}\n\n${prompt}` : prompt;
  const generationConfig = {
    temperature: 0.2
  };
  if (responseMimeType) generationConfig.responseMimeType = responseMimeType;
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      contents: [{ parts: [{ text: fullPrompt }] }],
      generationConfig
    }),
    signal: AbortSignal.timeout(timeoutMs)
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    const message = payload?.error?.message || `Gemini request failed (${response.status})`;
    const error = new Error(message);
    error.statusCode = 502;
    throw error;
  }
  const text = payload.candidates?.[0]?.content?.parts?.map((item) => item?.text || "").join("\n") || "";
  return {
    model,
    text
  };
}

async function runProviderTextCall(provider, apiKey, prompt, timeoutMs, options = {}) {
  const normalizedProvider = normalizeProviderName(provider);
  const baseOptions = {
    systemPrompt: options.systemPrompt,
    model: options.model
  };
  if (normalizedProvider === "openai") {
    return callOpenAi({
      apiKey,
      prompt,
      timeoutMs,
      ...baseOptions,
      responseFormat: options.responseFormat || null
    });
  }
  if (normalizedProvider === "anthropic") {
    return callAnthropic({ apiKey, prompt, timeoutMs, ...baseOptions });
  }
  if (normalizedProvider === "gemini") {
    return callGemini({
      apiKey,
      prompt,
      timeoutMs,
      ...baseOptions,
      responseMimeType: options.responseMimeType || null
    });
  }
  const err = new Error(`Unsupported provider '${provider}'`);
  err.statusCode = 400;
  throw err;
}

function sanitizeHtmlArtifact(raw = "", fallbackTitle = "Quick Tool") {
  const text = String(raw ?? "").trim();
  if (!text) return "";
  const match = text.match(/```html\s*([\s\S]*?)```/i) || text.match(/```([\s\S]*?)```/i);
  const html = (match?.[1] || text).trim();
  if (!/<html/i.test(html)) {
    return [
      "<!doctype html>",
      "<html><head><meta charset=\"utf-8\"/><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>",
      `<title>${fallbackTitle}</title>`,
      "<style>body{font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;padding:20px;background:#f6f8fb;color:#0f172a}pre{white-space:pre-wrap;border:1px solid #e2e8f0;border-radius:10px;padding:12px;background:#fff}</style>",
      "</head><body>",
      `<h2>${fallbackTitle}</h2>`,
      `<pre>${html.replaceAll("<", "&lt;").replaceAll(">", "&gt;")}</pre>`,
      "</body></html>"
    ].join("");
  }
  return html;
}

export async function generateSkillArtifactsWithLlm({
  provider,
  apiKey,
  answers = {},
  workspaceAgentName = "Titus",
  timeoutMs = 16000
}) {
  const normalizedProvider = normalizeProviderName(provider);
  if (!apiKey) {
    const err = new Error("apiKey is required for LLM generation");
    err.statusCode = 400;
    throw err;
  }
  const fallbackManifest = buildSkillManifestFromAnswers(answers);
  const prompt = buildLlmPrompt(answers, workspaceAgentName);
  const response = await runProviderTextCall(
    normalizedProvider,
    apiKey,
    prompt,
    timeoutMs,
    {
      systemPrompt: "You generate safe, policy-aware skill manifests for enterprise agents.",
      responseFormat: normalizedProvider === "openai" ? { type: "json_object" } : null,
      responseMimeType: normalizedProvider === "gemini" ? "application/json" : null
    }
  );

  const parsed = parseJsonFromModelText(response.text) || {};
  const manifestCandidate = parsed.manifest && typeof parsed.manifest === "object" ? parsed.manifest : parsed;
  const manifest = sanitizeManifest(manifestCandidate, fallbackManifest);
  const skillMarkdown = asString(parsed.skillMarkdown || parsed.skillMd || parsed.markdown) || buildSkillMarkdown(manifest);
  return {
    provider: normalizedProvider,
    model: response.model,
    manifest,
    skillMarkdown
  };
}

export async function generateWebToolFromPrompt({
  provider,
  apiKey,
  prompt = "",
  title = "Quick Tool",
  timeoutMs = 18000
}) {
  const normalizedProvider = normalizeProviderName(provider);
  if (!apiKey) {
    const err = new Error("apiKey is required for web tool generation");
    err.statusCode = 400;
    throw err;
  }
  const modelPrompt = [
    "Create a single-file production-quality HTML tool.",
    "Return ONLY the HTML document.",
    "Constraints:",
    "- No external dependencies.",
    "- Include modern CSS and any JS inline.",
    "- Mobile responsive.",
    "- Clear controls and labels.",
    "",
    `Tool title: ${title}`,
    `User prompt: ${String(prompt || "").trim()}`
  ].join("\n");
  const response = await runProviderTextCall(normalizedProvider, apiKey, modelPrompt, timeoutMs, {
    systemPrompt: "You build polished, production-quality web tools from product prompts."
  });
  const html = sanitizeHtmlArtifact(response.text, title);
  return {
    provider: normalizedProvider,
    model: response.model,
    html
  };
}

export async function generateChatReplyWithLlm({
  provider,
  apiKey,
  agentName = "Titus",
  userMessage = "",
  meContent = "",
  soulContent = "",
  effort = "high",
  planMode = false,
  threadTitle = "",
  timeoutMs = 16000
}) {
  const normalizedProvider = normalizeProviderName(provider);
  if (!apiKey) {
    const err = new Error("apiKey is required for LLM chat generation");
    err.statusCode = 400;
    throw err;
  }
  const compactSoul = String(soulContent || "").slice(0, 2200);
  const compactMe = String(meContent || "").slice(0, 1400);
  const compactThread = String(threadTitle || "").slice(0, 180);
  const compactMessage = String(userMessage || "").slice(0, 6000);
  const effortLabel = ["low", "medium", "high"].includes(String(effort || "").toLowerCase())
    ? String(effort).toLowerCase()
    : "high";

  const systemPrompt = [
    `You are ${agentName}, a collaborative firm-data copilot inside a team workspace.`,
    "Rules:",
    "- Be accurate and practical; do not fabricate numbers or events.",
    "- Keep response concise unless user asks for detail.",
    "- If request implies actions, provide clear next steps.",
    "- Respect policy/approval guardrails context.",
    `Reasoning effort preference: ${effortLabel}.`,
    planMode ? "- Return a concise numbered implementation plan." : "- Return a direct helpful answer."
  ].join("\n");

  const prompt = [
    compactThread ? `Thread: ${compactThread}` : "Thread: current workspace conversation",
    "",
    "User profile hints (me.md excerpt):",
    compactMe || "(none)",
    "",
    "Workspace operating rules (soul.md excerpt):",
    compactSoul || "(none)",
    "",
    "User message:",
    compactMessage,
    "",
    planMode
      ? "Return only the assistant reply text. Format as a numbered plan with clear, actionable steps."
      : "Return only the assistant reply text."
  ].join("\n");

  const response = await runProviderTextCall(normalizedProvider, apiKey, prompt, timeoutMs, {
    systemPrompt
  });
  return {
    provider: normalizedProvider,
    model: response.model,
    text: asString(response.text)
  };
}
