# InsightFoundry Plan Addendum

Date: 2026-02-18  
Scope: Visual direction update + free UI asset strategy + research-grounded problem expansion

## 1) Visual Direction: Apple Glass x Codex Clarity

### Product intent
- Keep Codex-like task clarity (high signal layout, low noise, command-first interaction).
- Layer in Apple-style material depth (translucent surfaces, blur, edge highlights, motion hierarchy).
- Preserve enterprise legibility first: no decorative blur that reduces comprehension.

### Design principles to enforce
- Workspace-first: user lands directly in a focused copilot/run canvas.
- Settings-first operations: all integrations, model preferences, report definitions, channels, and skills stay in Settings.
- Progressive depth: base neutral canvas + glass panels only where they improve grouping and focus.
- Accessibility guardrails:
  - Text contrast at WCAG AA minimum (4.5:1 body, 3:1 large text).
  - Blur/transparency fallback for reduced-support browsers.
  - "Reduced motion" behavior for animated transitions.

### UI implementation rules
- Use shared tokens for opacity, blur, border glow, and elevation.
- Separate panel types:
  - `solid` (forms, long text editing)
  - `glass` (context surfaces, side rails, overlays)
  - `quiet` (lists, metadata blocks)
- Keep command and run actions fixed and obvious (no hidden critical controls).

## 2) Free UI Package Strategy (No lock-in, license-safe)

### Adopt now
- `shadcn/ui` (MIT) as composable base components.
- `Radix Primitives` (MIT) for accessibility-critical interactions.
- `Recharts` (MIT) for fast dashboard charting in React pages.

### Optional packs
- `Mantine` (MIT) for high-velocity admin/settings forms where needed.
- `Tremor` (Apache-2.0) for dashboard blocks if we need faster chart assembly.
- `Apache ECharts` (Apache-2.0) for heavy-volume or advanced analytics visualizations.

### Rules for package adoption
- No UI package that forces runtime lock-in for design tokens.
- All imported components must map to InsightFoundry token system.
- License metadata tracked in `docs/THIRD_PARTY_UI_LICENSES.md` before production enablement.

## 3) Research Findings: Common Data Digestion Complaints

### Recurring pain patterns across firms/agencies/startups
1. Data quality and trust collapse decision confidence.
- dbt 2024 and 2025 reports keep poor data quality as the top analytics challenge, with ~56%+ of respondents flagging it.

2. Fragmented tools and silos block action.
- Salesforce 2026 State of Data/Analytics: major concern around trapped/siloed data and weak governance coverage.
- HubSpot state-of-marketing data highlights disconnected systems and poor cross-team data flow.

3. Too many dashboards, too little decision utility.
- Teams can produce dashboards quickly but struggle to convert output into decisions and clear actions.

4. Execution friction from context switching.
- HBR app-toggling research indicates substantial reorientation overhead from constant tool switching.

5. AI reliability and explainability remain blockers.
- McKinsey 2025: many AI-using organizations report negative consequences, with inaccuracy among top issues.

6. Founder/startup pressure is now "convert or die."
- Slush 2025 founder survey: fundraising stress remains high, but conversion and revenue growth bottlenecks are rising sharply.

### Interpretation for InsightFoundry
- Users do not need more charts; they need:
  - trusted metric definitions,
  - confidence-scored interpretation,
  - explicit recommendation + approval path,
  - single run timeline from source freshness to delivered output.

## 4) Product Plan Changes (Actionable)

### A. Workspace changes (priority)
- Add command bar (`Cmd+K`) with actions:
  - New Analysis Run
  - Check Source Freshness
  - Preview Report
  - Deliver to Channel
  - Open Skill Builder
- Promote "Run Evidence" and "Why this recommendation" sections by default.
- Add confidence + data freshness badges in run context header.

### B. Settings enhancements
- `Connections`: include "freshness SLA", "quality checks", and "owner".
- `Models`: add profile presets per domain with default routing policy.
- `Reports`: reusable report kits with objective-to-template defaults.
- `Skills`: wizard + JSON editor + validation + test sandbox trace.
- `Channels`: explicit test delivery and template preview.

### C. Run engine behavior
- Run step contract becomes mandatory:
  - Source Freshness Check
  - Quality Gate
  - Model Run
  - Insight Synthesis
  - Report Build
  - Delivery
- Any failure returns deterministic remediation guidance, not generic errors.

### D. Multi-tenant operating model
- Blueprint packs expanded:
  - `Starter Marketing Ops`
  - `Starter Finance Ops`
  - `Founder GTM Cockpit` (for high-velocity startups)
- Each blueprint ships:
  - default connections
  - model profile set
  - report kit set
  - policy preset
  - skill starter pack

## 5) Immediate Build Sequence (next 2 implementation sprints)

### Sprint G1: UX and visual baseline
- Implement glass token system and panel styles in app shell.
- Add command bar and quick actions.
- Improve first-load empty states and checklist visibility.
- Add accessibility pass for contrast and keyboard navigation.

### Sprint G2: Usability hardening and outcome clarity
- Add run evidence drawer with input/output trace.
- Add source freshness + quality badges across Workspace/Runs.
- Add report and channel output previews before execute/deliver.
- Add "founder mode" report template optimized for growth + cash + conversion.

## 6) Web Research Sources

- dbt Labs: [2024 State of Analytics Engineering](https://www.getdbt.com/resources/state-of-analytics-engineering-2024)
- dbt Labs: [2025 State of Analytics Engineering](https://www.getdbt.com/resources/state-of-analytics-engineering-2025)
- Salesforce: [State of Data and Analytics 2026 summary](https://www.salesforce.com/news/stories/data-analytics-trends-2026/image-93/)
- McKinsey: [The State of AI: Global Survey 2025](https://www.mckinsey.com/capabilities/quantumblack/our-insights/the-state-of-ai)
- Gartner: [CMO strategic dysfunction survey (2025)](https://www.gartner.com/en/newsroom/press-releases/2025-03-25-gartner-survey-reveals-84-percent-of-cmos-report-high-levels-of-strategic-dysfunction)
- HubSpot: [State of Marketing trends report](https://blog.hubspot.com/marketing/hubspot-blog-marketing-industry-trends-report)
- Harvard Business Review: [App toggling/context switching analysis](https://hbr.org/2022/08/how-much-time-and-energy-do-we-waste-toggling-between-applications)
- Slush: [Startup Struggle Survey 2025](https://slush.org/newsroom/slush-startup-struggle-survey-2025)
- Apple Developer: [Liquid Glass overview](https://developer.apple.com/documentation/technologyoverviews/liquid-glass)
- MDN: [backdrop-filter](https://developer.mozilla.org/docs/Web/CSS/backdrop-filter)
- W3C WAI: [WCAG contrast minimum understanding](https://www.w3.org/WAI/WCAG21/Understanding/contrast-minimum)
- shadcn/ui: [GitHub (MIT)](https://github.com/shadcn-ui/ui)
- Radix Primitives: [GitHub (MIT)](https://github.com/radix-ui/primitives)
- Recharts: [GitHub (MIT)](https://github.com/recharts/recharts)
- Mantine: [v7 docs (MIT)](https://v7.mantine.dev/)
- Tremor: [GitHub (Apache-2.0)](https://github.com/tremorlabs/tremor)
- Apache ECharts: [Download + license](https://echarts.apache.org/en/download.html)
