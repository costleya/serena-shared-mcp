# serena-shared-mcp

`serena-shared` starts or reuses one local Serena Streamable HTTP server per
Git checkout and exposes it to stdio-only MCP clients. Multiple clients in the
same checkout share Serena's language servers while retaining independent MCP
connections.

State is stored in the repository's common Git directory at
`.git/serena-shared`, so linked worktrees remain isolated from one another.

## Requirements

- macOS or Linux
- Python 3.14
- [uv](https://docs.astral.sh/uv/)
- Serena available on `PATH`

Install Serena independently, following its upstream recommendation:

```sh
uv tool install -p 3.13 serena-agent
```

## Install

After publication:

```sh
uv tool install -p 3.14 serena-shared-mcp
```

From this checkout:

```sh
uv tool install -p 3.14 .
```

## MCP client configuration

Configure a stdio MCP client to run the following command from the target Git
checkout:

```json
{
  "command": "serena-shared",
  "args": ["proxy", "--idle-timeout-minutes", "5"]
}
```

`proxy` is the default command, and the idle timeout defaults to 5 minutes, so
an empty argument list is equivalent. The lightweight stdio proxy stays
connected, while the heavier Serena and language-server processes stop after
the configured period without completed requests. A later request starts and
reinitializes Serena transparently. Startup and discovery remain lightweight:
the backend starts without a project or language server, and the proxy activates
its checkout only for the first real `tools/call` in each backend generation.
This requires a Serena context that exposes `activate_project`. Contexts with
`single_project: true` are unsupported by the lazy proxy: Serena fixes their
reduced tool set at startup and disables project switching, so activating after
discovery cannot provide an equivalent tool surface.

## CLI and Serena pass-through

The wrapper owns only the `proxy` and `status` commands, `--profile-key`, and
`--idle-timeout-minutes`. It does not inject a Serena context, mode, dashboard,
browser, onboarding, or memories setting: Serena's native defaults apply.

Pass Serena arguments after an explicit `--` separator:

```sh
serena-shared proxy
serena-shared proxy -- --context desktop-app
serena-shared proxy --profile-key readonly -- --context ./readonly.yml
```

The wrapper rejects these Serena tail flags because it owns the checkout and
loopback transport: `--project`, `--project-file`, `--project-from-cwd`,
`--transport`, `--host`, and `--port`. Their `--flag=value` forms are rejected
as well.

A backend identity is only the Git checkout and a nullable profile key. Within
one checkout and key, the first live backend's forwarded Serena arguments win.
When that backend is restarted, the invocation that triggers the restart
supplies its current forwarded arguments. Different profile keys run
independently. The idle timeout defaults to 5 minutes.

## MCP version compatibility

`serena-shared-mcp` itself uses the stable MCP Python SDK v2. It supports both
the MCP 2026-07-28 request envelope and the older initialization flow used by
MCP Python SDK v1 servers.

Here, **legacy means MCP SDK v1**, not the removed Node implementation. During
development, Serena Agent 1.6.1 was verified running on Python 3.13 with MCP
SDK 1.28.1. The bridge uses the v2 client's automatic negotiation and fallback
so it works with that Serena release while remaining ready for Serena servers
that use MCP SDK v2.

## Status

```sh
serena-shared status
```

`status` lists the registered profile keys for the current checkout, including
records that are not healthy. Records belonging to other checkouts are ignored.

Process and state cleanup is automatic; there are no manual stop or
garbage-collection commands. Agent tool allowlists remain the responsibility
of each MCP client.

## Development

Install dependencies and run the normal test suite:

```sh
uv sync --locked
uv run pyright
uv run pytest
```

The two end-to-end tests are opt-in because they inspect child processes and
open local loopback sockets. The MCP v1 compatibility test also needs the
Python interpreter from an independently installed Serena tool:

```sh
SERENA_SHARED_INTEGRATION=1 \
SERENA_SHARED_LEGACY_PYTHON="$(uv tool dir)/serena-agent/bin/python3" \
uv run pytest
```

The end-to-end suite verifies both MCP SDK v1 and v2 HTTP backends, including
two independent stdio clients sharing one Serena backend without sharing their
MCP connections.

Build the wheel and source distribution with:

```sh
uv build
```
