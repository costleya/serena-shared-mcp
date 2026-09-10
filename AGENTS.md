# Repository Guidelines

## Task Execution and Communication

Carry authorized work through implementation and verification. Resolve questions from repository evidence before asking the user; ask only when missing information materially changes the outcome. Respect collaboration mode and permission boundaries. Keep updates concise and report findings, verification, and material limitations.

## Project Structure & Module Organization

This Python 3.14 package uses a `src/` layout. In `src/serena_shared/`, `cli.py` defines the command, `lifecycle.py` manages checkout state, and `transport.py` bridges stdio to Serena HTTP. Packaged configuration is under `resources/`; tests are in `tests/`. Do not commit `dist/` artifacts.

## Build, Test, and Development Commands

- `uv sync --locked` installs dependencies from `uv.lock`.
- `uv run pyright` performs strict type checking.
- `uv run pytest` runs unit tests; integrations skip by default.
- `uv run pytest --cov=serena_shared --cov-branch` reports branch coverage.
- `SERENA_SHARED_INTEGRATION=1 SERENA_SHARED_LEGACY_PYTHON="$(uv tool dir)/serena-agent/bin/python3" uv run pytest` runs the full socket/process suite, including MCP v1 compatibility.
- `uv build` creates the wheel and source distribution.

Run `uv run serena-shared status` for an observational health check. Use the default `proxy` command for MCP access; cleanup is automatic.

## Coding Style & Naming Conventions

Use four-space indentation, `snake_case` functions, `PascalCase` classes, and `UPPER_CASE` constants. Keep postponed annotations, explicit return types, and narrow typed structures. Pyright is strict; prefer precise guards over suppressions. No formatter is configured, so match nearby code.

## Testing Guidelines

For documentation, instructions, or tool configuration only, check relevant formatting, parse changed configuration, and review instruction consistency. Do not run application tests or builds unless behavior changes.

Use pytest and name files/functions `test_*.py` and `test_*`. Add focused regression tests beside the affected module. Use `tmp_path` and monkeypatching; never depend on live Serena state. Gate tests requiring sockets or process inspection as integrations. Branch coverage is enabled without a numeric threshold.

## Code Navigation

Use repository-local code intelligence before broad source inspection. Start with GrepAI when behavior, flow, or file ownership is unknown; it runs in single-project mode, so call searches without a workspace or project argument. Use Serena for known symbols, references, implementations, diagnostics, and scoped edits. Use targeted `rg` searches for literal strings, filenames, configuration, and generated files. If a semantic tool fails, retry once with a corrected request before falling back.

## Subagent Orchestration

Use the main thread as coordinator, synthesis owner, and final acceptance owner. Delegate independent work when it reduces completion time or improves evidence or review quality. Handle small, single-owner work directly.

### Role Routing

| Role | Responsibility |
| --- | --- |
| `repo_explorer` | Read-only discovery of unfamiliar behavior, dependencies, risks, and edit surfaces. |
| `researcher` | Current primary-source evidence for Python, MCP, Serena, packaging, and platforms. |
| `repo_implementer` | Production code and package configuration within an assigned scope. |
| `test_implementer` | Independent pytest tests, fixtures, mocks, and test configuration. |
| `test_runner` | Post-integration verification after all relevant writers finish, without repairs. |
| `code_reviewer` | Routine independent review at Sol Medium before acceptance. |
| `code_reviewer_deep` | Materially high-risk process safety, concurrency, lifecycle, transport, or architectural review at Astra Medium. |

### Workflow Rules

1. **Delegate independent work early.** Dispatch useful, stable, non-overlapping assignments concurrently. Sequence work with real dependencies; avoid duplicate investigation unless deliberate independent replication resolves a concrete uncertainty.
2. **Define ownership and contracts.** State the objective, acceptance criteria, public behavior, owned files or symbols, constraints, relevant paths, and expected evidence. For every named specialist spawn, explicitly set `fork_turns = "none"` and provide a standalone prompt.
3. **Route by responsibility.** Keep production implementation, test authoring, verification, and review ownership separate. The main thread coordinates rather than duplicates active assignments.
4. **Separate ownership.** Multiple agents of the same role may own disjoint scopes. Never assign simultaneous writers to the same file or symbol. Preserve unrelated user and agent edits.
5. **Honor assignment lifecycle.** A wait timeout or absence of edits is not evidence that an agent is stuck. Continue independent work, send bounded status requests when useful, and return repair work to its owner with `followup_task`. Interrupt only for cancellation, unsafe actions, ownership conflicts, material scope corrections, or reported blockers. Inspect partial work before reassignment.
6. **Parallelize stable production and tests.** Author concurrently when contracts are stable and ownership is disjoint; otherwise establish the contract first. Test authors working alongside production may check syntax or harness setup, but must treat behavioral results as provisional and report the final command for the runner.
7. **Enforce an integration barrier.** Wait for all relevant writers before final verification. Use one `test_runner` for the complete integrated gate after parallel writing or for broad, slow, or artifact-producing checks. A small focused check may remain with the main thread. Runners report evidence and likely ownership; repair returns to the writer.
8. **Review by risk domain.** Use one `code_reviewer` for routine non-trivial integrated changes, or `code_reviewer_deep` for materially high-risk work. Add reviewers only for clearly disjoint evidence and risk domains. Return corrections to the same reviewer and let the main thread resolve disagreements.
9. **Keep ownership bounded.** Specialists follow their role's delegation rules and stay within the assignment. Use fresh agents for unrelated follow-on work or a material architectural pivot, with a concise current-state handoff.

MCP servers and permissions are inherited from the parent task. Configure shared MCP servers in `.codex/config.toml`; role files define models, reasoning, instructions, and supported feature reductions. Read-only and external-research responsibilities are instruction-level contracts, not role-specific MCP or filesystem isolation.

## Git

- Write Conventional Commit subjects in the format `type(scope): subject`
- Use a lowercase imperative subject, the narrowest meaningful scope, and a short description of the change
- Do not auto-push. When the user explicitly authorizes a push for completed GitHub issue work and the push succeeds, close the associated issue unless the user asked to leave it open
- Summaries to issues should mention what changed, how acceptance criteria were met, and any notable design decisions

## Safety & State Management

Preserve per-worktree isolation, loopback-only endpoints, and process-fingerprint validation. Never terminate unrelated Serena processes.
