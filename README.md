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
  "args": ["proxy"]
}
```

`proxy` is the default command, so an empty argument list is equivalent.

## MCP version compatibility

`serena-shared-mcp` itself uses the stable MCP Python SDK v2. It supports both
the MCP 2026-07-28 request envelope and the older initialization flow used by
MCP Python SDK v1 servers.

Here, **legacy means MCP SDK v1**, not the removed Node implementation. During
development, Serena Agent 1.6.1 was verified running on Python 3.13 with MCP
SDK 1.28.1. The bridge uses the v2 client's automatic negotiation and fallback
so it works with that Serena release while remaining ready for Serena servers
that use MCP SDK v2.

## Maintenance

```sh
serena-shared status
serena-shared stop
serena-shared gc
```

- `status` reports the current checkout's registry record and health.
- `stop` signals only a process whose PID, start time, command, checkout, and
  port match the stored record.
- `gc` removes dead records and safely handles verified orphan processes and
  records for deleted worktrees.

The packaged Serena context disables project switching and memories. Agent
tool allowlists remain the responsibility of each MCP client.

## Development

Install dependencies and run the normal test suite:

```sh
uv sync --locked
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
