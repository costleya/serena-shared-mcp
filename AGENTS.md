# Repository Guidelines

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

Use pytest and name files/functions `test_*.py` and `test_*`. Add focused regression tests beside the affected module. Use `tmp_path` and monkeypatching; never depend on live Serena state. Gate tests requiring sockets or process inspection as integrations. Branch coverage is enabled without a numeric threshold.

## Code Navigation

Use repository-local code intelligence before broad source inspection. Start with GrepAI when behavior, flow, or file ownership is unknown; it runs in single-project mode, so call searches without a workspace or project argument. Use Serena for known symbols, references, implementations, diagnostics, and scoped edits. Use targeted `rg` searches for literal strings, filenames, configuration, and generated files. If a semantic tool fails, retry once with a corrected request before falling back.

## Multi-Agent Workflow

The main task coordinates work, synthesizes evidence, and owns final acceptance. Handle simple questions, documentation edits, and focused checks directly. Delegate only when a specialist materially helps, and give every agent an explicit outcome, ownership boundary, constraints, and expected evidence.

### Role Routing

| Role | Responsibility |
| --- | --- |
| `repo_explorer` | Trace unfamiliar behavior, dependencies, risks, and likely edit surfaces without writing. |
| `researcher` | Find current primary-source evidence for Python, MCP, Serena, packaging, or platform questions. |
| `repo_implementer` | Change production code and package configuration within an assigned scope. |
| `test_implementer` | Independently author pytest tests, fixtures, mocks, and test configuration. |
| `test_runner` | Verify the integrated worktree after all relevant writers finish; never repair failures. |
| `code_reviewer` | Review completed changes for correctness, regressions, process safety, compatibility, and test gaps. |

Multiple explorers or researchers may run in parallel when their questions do not overlap. The main task must reconcile their evidence before assigning implementation.

### Orchestration Rules

1. **Gather evidence when needed.** Use `repo_explorer` for repository uncertainty and `researcher` for external uncertainty. Do not delegate when the edit and validation path are already obvious.
2. **Define ownership and contracts.** State acceptance criteria, public behavior, owned files or symbols, prohibited scope, and required verification. Use `fork_turns = "none"` for named specialists and make prompts self-contained.
3. **Separate production and tests.** Production files belong to `repo_implementer`; tests and test infrastructure belong to `test_implementer`. Neither should edit the other's area.
4. **Parallelize only stable work.** Production and tests may be authored concurrently only when acceptance criteria and public contracts are stable and file ownership is disjoint. Otherwise establish production behavior first.
5. **Treat pre-integration results as provisional.** A test author working alongside production may validate syntax and harness setup, but must defer behavioral conclusions and report the exact final command.
6. **Enforce an integration barrier.** Wait for every writing agent to finish before final verification. Use `test_runner` after parallel writers, or whenever checks are broad, slow, artifact-producing, or worth independently repeating. A small focused check may remain with the main task.
7. **Review and accept.** Send non-trivial integrated changes and verification evidence to `code_reviewer`. Route findings back to the correct owner, rerun affected checks, and let the main task resolve disagreements against the stated requirements.

Test runners report commands, exit status, evidence, and likely ownership; they do not diagnose deeply or edit source. Reviewers produce actionable findings, not implementation. No specialist may delegate further or expand beyond its assignment.

## Git

- Write Conventional Commit subjects in the format `type(scope): subject`
- Use a lowercase imperative subject, the narrowest meaningful scope, and a short description of the change
- Do not auto-push. When the user explicitly authorizes a push for completed GitHub issue work and the push succeeds, close the associated issue unless the user asked to leave it open
- Summaries to issues should mention what changed, how acceptance criteria were met, and any notable design decisions

## Safety & State Management

Preserve per-worktree isolation, loopback-only endpoints, and process-fingerprint validation. Never terminate unrelated Serena processes.
