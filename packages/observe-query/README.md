# phlo-observe-query

Typed, read-only client for the phlo-observer V2 query API (spec §19).

```python
from observe_query import ObserverClient

client = ObserverClient("http://observer:8080", token="...")

run = client.run("01J...")
failures = run.failures()
changes = run.recent_changes()
impact = run.affected_downstream()
bundle = run.investigate()
```

## Agent tools

`observe_query.tools` exposes the spec §19.2 tool surface as a dispatch
table; every function takes the client first and returns structured JSON:

```python
from observe_query import AGENT_TOOLS, call_tool

result = call_tool(client, "get_asset_health", entity_id="asset://a/b")
```

Agent access is read-only by design (spec §19.4): this package exposes no
write or action endpoints.
