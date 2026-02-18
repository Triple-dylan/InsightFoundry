# InsightFoundry UX Workflow Benchmarks

Date: 2026-02-18

## Objective

Translate proven navigation/workflow patterns from Codex-style AI tooling and modern collaborative work apps into InsightFoundry's product UX:

- Clear first action in under 30 seconds
- Settings-first operations for integrations and governance
- Shared, thread-based collaboration for data work
- Fast keyboard and command-driven flow

## External UX References

1. OpenAI Codex product page:
- [OpenAI Codex](https://openai.com/codex/)
- Principle used: center the primary execution loop (prompt -> run -> output) with explicit action affordances.

2. Notion help center:
- [Create and edit pages](https://www.notion.com/help/create-and-edit-pages)
- [Comments, mentions, and reminders](https://www.notion.com/help/comments-mentions-and-reminders)
- Principle used: hierarchical sidebar organization plus collaborative inline discussion.

3. Linear docs:
- [Keyboard shortcuts](https://linear.app/docs/keyboard-shortcuts)
- Principle used: command-first and keyboard-first navigation to reduce friction and context switching.

4. Slack help center:
- [Use threads to organize discussions](https://slack.com/help/articles/115000769927-Use-threads-to-organize-discussions-in-channels)
- Principle used: persistent threaded discussion instead of mixed channel noise.

## UX Principles Adopted

1. One primary center workflow
- The center pane is chat-first and run-aware.
- Side panes support context, not compete with the main loop.

2. Progressive operations
- Left rail: organize context (folders, threads).
- Center: do work (chat, commands, attachments).
- Right rail: configure/inspect execution state (run setup, evidence, actions, comments).

3. Settings as control plane
- All channel integrations (Slack/Telegram), model/credentials, and team membership are managed from Settings.

4. Shared-by-default collaboration
- Foldered thread system with tenant-scoped comments.
- Team section to define collaborators and roles.

5. Command + discoverability
- Command input supports run and navigation actions.
- Direct shortcuts to open settings areas (models/channels/team).

## Implemented in This Iteration

1. Backend collaboration APIs
- `GET/POST /v1/settings/team`
- `PATCH /v1/settings/team/{memberId}`
- `GET/POST /v1/workspace/folders`
- `PATCH /v1/workspace/folders/{folderId}`
- `GET/POST /v1/workspace/threads`
- `GET /v1/workspace/threads/{threadId}`
- `GET/POST /v1/workspace/threads/{threadId}/comments`

2. New tenant-scoped collaboration state
- Team members
- Workspace folders
- Workspace threads
- Thread comments

3. Workspace UI changes
- Left rail now uses folder + thread hierarchy.
- Center chat is tied to active thread and supports shared comments.
- Chat toolbar supports model selection and settings shortcuts.
- Quick file/screenshot attachment UX (drag/paste/file-pick path via composer).
- Right rail adds `Comments` context tab.

4. Settings IA expansion
- Added `Team` section.
- Slack/Telegram integration remains under `Settings > Channels`.

5. Safety and isolation
- Collaboration APIs enforce tenant-scoped reads/writes.
- Added integration test for cross-tenant thread access denial.

## Next UX Hardening Steps

1. Add true command palette overlay (`Cmd+K`) with fuzzy search and action registry.
2. Add per-thread run context pinning (a thread can bind to one active run context).
3. Add comment mentions (`@name`) and unread badges on folders/threads.
4. Add file artifact persistence API (currently attachment UX is local-first).
5. Add role-based thread permissions (comment-only vs run-operator capabilities).
