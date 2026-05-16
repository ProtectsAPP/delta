# Observability — Grafana + Prometheus

Drop-in monitoring for `delta_server`. Three artefacts ship with this
directory:

- `dashboards/delta.json` — Grafana 9.x / 10.x dashboard JSON.
  Five rows: Overview, HTTP traffic & latency, Storage / cache / vectors,
  WebSocket & DeltaQL traffic. Templated on `instance` so it works for
  single-node and sharded fleets alike.
- `alerts/delta.yml` — Prometheus alerting rules covering availability,
  error rate, latency, cache hit rate, compaction backlog, connection
  pool saturation and security bursts.
- `dashboards/prometheus.example.yml` — minimal scrape config so the
  dashboard "just works" out of the box.

## 1 · Make Prometheus scrape `delta_server`

The server already serves `GET /metrics` in Prometheus text format —
no exporter required. Point Prometheus at every node:

```yaml
# /etc/prometheus/prometheus.yml — minimal example.
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - "/etc/prometheus/alerts/delta.yml"

scrape_configs:
  - job_name: delta
    metrics_path: /metrics
    static_configs:
      - targets:
          - "delta-n1:16888"
          - "delta-n2:16888"
          - "delta-n3:16888"
```

For a sharded cluster, list every node — the `instance` label flows
into the dashboard's selector automatically.

## 2 · Import the Grafana dashboard

Either:

- **Grafana UI**: Dashboards → Import → upload `dashboards/delta.json`.
  Pick your Prometheus datasource at the top of the import wizard.
- **Provisioning** (recommended for production): copy the JSON into
  `/var/lib/grafana/dashboards/delta.json` and add a provisioning
  config:

```yaml
# /etc/grafana/provisioning/dashboards/delta.yaml
apiVersion: 1
providers:
  - name: delta
    type: file
    folder: Delta
    options:
      path: /var/lib/grafana/dashboards
    allowUiUpdates: false
```

Restart Grafana once and the "Delta — Overview" dashboard appears under
the *Delta* folder. The `instance` template variable lets you pivot
across nodes.

## 3 · Wire in the alerts

Drop `alerts/delta.yml` into Prometheus's `rule_files` (see the snippet
above). Validate before reloading:

```sh
promtool check rules alerts/delta.yml
curl -X POST http://prometheus:9090/-/reload
```

The rules use only labels Prometheus emits by default (`instance`,
`job`) — no relabelling needed.

| Severity   | Where it goes                                        |
|------------|------------------------------------------------------|
| `page`     | Wake-up routes (PagerDuty, Opsgenie, Alertmanager)   |
| `warning`  | Dashboard banner + ticketing system                  |
| `info`     | Silent record (e.g. `up` flapping during deploys)    |

The Alertmanager routing config is intentionally not shipped here —
every shop has its own paging conventions.

## 4 · Customising

The dashboard is plain JSON; edit it in Grafana's UI and "Save as JSON"
back into this file. Add new panels for any custom `delta_*` metric
emitted via the `set_traffic_hook(...)` extension point in
`src/network/http_server.hpp`. The histogram panel uses
`histogram_quantile(...)` over `delta_http_request_duration_ms_bucket`,
so any new buckets you add (see `LatencyHistogram::BUCKETS`) require no
dashboard changes.

When tightening thresholds in `alerts/delta.yml`, prefer raising
`for:` durations over lowering `>` numbers — short flaps from rolling
restarts and compaction stutters cause false pages otherwise.
