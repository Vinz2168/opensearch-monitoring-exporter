# opensearch-monitoring-exporter

OpenSearch plugin that exports node-internal metrics directly to a dedicated
OpenSearch monitoring cluster, **bypassing Data Prepper**.

Documents are written in **ss4o** format (Simple Schema for Observability),
compatible with OpenSearch Dashboards and with the index pattern used by Data Prepper.

```
OpenSearch cluster (production)
  └── telemetry-otel (metrics framework, experimental)
  └── monitoring-exporter-plugin  ← this plugin
         │
         │ Bulk API HTTPS
         ▼
OpenSearch cluster (monitoring)
  └── indices: ss4o_metrics-opensearch-yyyy.MM.dd
```

---

## Prerequisites

- OpenSearch 3.7.0 (experimental telemetry feature)
- Java 21+
- No external build tool needed — the project uses the Gradle wrapper (`./gradlew`)

---

## Build

```bash
./gradlew build
# Output: build/distributions/opensearch-monitoring-exporter.zip
```

---

## Installation

```bash
bin/opensearch-plugin install \
  file:///path/to/opensearch-monitoring-exporter.zip
```

---

## Configuration (opensearch.yml)

### Minimal

```yaml
# OpenSearch feature flag (requires restart, static setting)
opensearch.experimental.feature.telemetry.enabled: true
telemetry.feature.metrics.enabled: true

# Plugin
monitoring.exporter.target.hosts: ["https://monitoring-cluster:9200"]
monitoring.exporter.target.username: monitor_writer
```

### Password (in the keystore — never in plaintext in opensearch.yml)

```bash
bin/opensearch-keystore add monitoring.exporter.target.password
```

### TLS with self-signed certificate (typical RKE2/NiFiKop environment)

The monitoring cluster almost certainly uses certificates generated
by the OpenSearch Operator (self-signed or internal CA).

**Step 1** — Export the CA certificate from the K8s Secret:

```bash
kubectl get secret opensearch-monitoring-cert \
  -n monitoring \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > monitoring-ca.crt
```

**Step 2** — Import into a JKS truststore:

```bash
keytool -importcert \
  -alias monitoring-ca \
  -file monitoring-ca.crt \
  -keystore /etc/opensearch/certs/monitoring-truststore.jks \
  -storepass changeit \
  -noprompt
```

**Step 3** — Mount the truststore on the pod (via Secret or ConfigMap) and configure:

```yaml
monitoring.exporter.tls.truststore.path: /etc/opensearch/certs/monitoring-truststore.jks
monitoring.exporter.tls.verify_hostname: true
```

```bash
bin/opensearch-keystore add monitoring.exporter.tls.truststore.password
```

### mTLS (if the monitoring cluster requires a client certificate)

```yaml
monitoring.exporter.tls.keystore.path: /etc/opensearch/certs/client-keystore.jks
```

```bash
bin/opensearch-keystore add monitoring.exporter.tls.keystore.password
```

### Development only (skip TLS verification)

```yaml
monitoring.exporter.tls.verify_hostname: false
```

---

## All settings

| Setting | Default | Description |
|---------|---------|-------------|
| `monitoring.exporter.target.hosts` | `["https://localhost:9200"]` | Monitoring cluster host(s) |
| `monitoring.exporter.target.username` | `admin` | Basic Auth username |
| `monitoring.exporter.target.password` | — | Password (keystore) |
| `monitoring.exporter.tls.verify_hostname` | `true` | Verify TLS hostname |
| `monitoring.exporter.tls.truststore.path` | — | JKS truststore path (self-signed) |
| `monitoring.exporter.tls.truststore.password` | — | Truststore password (keystore) |
| `monitoring.exporter.tls.keystore.path` | — | JKS keystore path (mTLS, optional) |
| `monitoring.exporter.tls.keystore.password` | — | mTLS keystore password (keystore) |
| `monitoring.exporter.index` | `ss4o_metrics-opensearch-%{yyyy.MM.dd}` | Destination index pattern |
| `monitoring.exporter.flush.interval_seconds` | `30` | Flush interval (5–300s) |
| `monitoring.exporter.batch.size` | `500` | Documents per BulkRequest |
| `monitoring.exporter.queue.capacity` | `10000` | In-memory queue (static) |
| `monitoring.exporter.retry.max` | `3` | Retries on HTTP error |

---

## ss4o document structure

```json
{
  "@timestamp": "2026-06-08T10:00:00.000Z",
  "name": "jvm.memory.heap.used",
  "description": "JVM heap memory used",
  "unit": "By",
  "kind": "GAUGE",
  "value": 123456789,
  "opensearch": {
    "name": "opensearch-node-1",
    "cluster": "prod-cluster"
  },
  "instrumentationScope": {
    "name": "org.opensearch.telemetry",
    "version": "2.17.0"
  },
  "attributes@jvm@memory@pool@name": "G1 Old Gen"
}
```

> **Note on dots in attributes**: OpenSearch interprets `.` as nested fields.
> The plugin replaces `.` with `@` in attributes (e.g. `jvm.memory.pool.name`
> → `attributes@jvm@memory@pool@name`), aligning with Data Prepper's behavior.

---

## TODO before production release

- [x] Add ss4o index template on the monitoring cluster
      (`index-templates/ss4o-metrics-opensearch-template.json`)
- [x] Integration tests against embedded OpenSearch (`OpenSearchSingleNodeTestCase`,
      see `MonitoringExporterIT`)
- [ ] Expose plugin-internal metrics via the node's Prometheus endpoint

---

## Architectural notes on metrics collection

Data collection (`StatsCollector`) is based on OpenSearch's standard APIs
(`ClusterHealthRequest`, `NodesStatsRequest`, `IndicesStatsRequest`, `ClusterStateRequest`),
with scheduled polling configurable via `monitoring.exporter.collect.interval_seconds`.

This is a deliberate architectural choice, not a shortcut: it's the same data model
used by the official [`opensearch-prometheus-exporter`](https://github.com/opensearch-project/opensearch-prometheus-exporter)
plugin (which exposes the same Stats APIs in Prometheus text format on demand, instead of
serializing them to ss4o and pushing them via the Bulk API as this plugin does). The Stats
APIs are stable and part of OpenSearch's public core, without the breaking-change risk of
still-experimental features.

### Possible future evolution

OpenSearch exposes (behind the experimental feature flag
`opensearch.experimental.feature.telemetry.enabled`) a native OpenTelemetry-based telemetry
framework, via the `TelemetryPlugin` extension point. A possible evolution of this plugin
is to register a custom `OtelMetricsRegistry`, read periodically by a `PeriodicMetricReader`,
to capture event-driven metrics (including distributions/histograms) directly from core
code paths already instrumented with OTel, instead of relying on periodic Stats API
snapshots.

This path has not been adopted in this version because the API is still labeled
experimental (it can change even in minor releases) — even the official community
Prometheus plugin does not use it today. It will be revisited once the API stabilizes,
keeping the Stats-based collector as a compatibility mode.

---

## Comparison with other OpenSearch monitoring approaches

Using the same requirements framework as
[opensearch-project/OpenSearch#18529](https://github.com/opensearch-project/OpenSearch/issues/18529)
(the RFC comparing telemetry-extraction options for OpenSearch), here is where
`opensearch-monitoring-exporter` fits relative to the four approaches it discusses.

| Requirement | Aiven Prometheus Exporter (in-process) | Prometheus Elasticsearch Exporter (sidecar) | Native OTel Telemetry Framework | Performance Analyzer | **opensearch-monitoring-exporter** |
|---|---|---|---|---|---|
| **Export telemetry & monitor clusters effectively** | Node metrics only | Extensive metrics, external | Metrics + traces (long-term goal), needs a Collector/Data Prepper hop to land anywhere | Real-time diagnostics (RCA, PerfTop), short retention (7–60 min) | Metrics only, same Stats APIs as Aiven's plugin, written durably to a monitoring cluster |
| **Minimal performance impact** | Runs in-process; low overhead | Runs externally; minimal impact on the OpenSearch cluster | In-process instrumentation; overhead depends on maturity | Shared-memory, designed for low overhead | In-process; batched Bulk writes with a backpressure-aware queue |
| **Flexible export mechanisms** | Scraping endpoint only | Scraping endpoint only | OTLP-native, needs a Collector or Data Prepper | Proprietary RCA/gRPC protocol | Push via Bulk API directly to OpenSearch, no scraper/Collector/Data Prepper hop |
| **Integration with telemetry framework / OpenSearch Dashboards** | Prometheus/Grafana-native; not directly queryable from Dashboards | Prometheus/Grafana-native; not directly queryable from Dashboards | End state UX: OTLP → Collector/Data Prepper → **ss4o** in Dashboards' Observability → Metrics UI | Its own PerfTop UI, not Dashboards | Writes **ss4o** documents directly — lands in the *same* shape Dashboards' Metrics UI expects today, without OTLP/Collector/Data Prepper |
| **Deployment & maintenance overhead** | No extra deployment if bundled with the distribution | Requires a separate service to deploy/scale | Requires an OTel Collector or Data Prepper pipeline | Ships with OpenSearch, own storage/UI to operate | One plugin + one destination OpenSearch cluster; no extra service |
| **API stability** | Stable Admin Client APIs (NodesStats/ClusterHealth/ClusterState/IndicesStats) | Stable REST Stats APIs | Experimental (`telemetry.feature.metrics.enabled`), can change in minor releases | Stable, purpose-built APIs | Same stable Admin Client APIs as Aiven's plugin — no coupling to the experimental telemetry framework |

**Summary:** closest in architecture to the **Aiven Prometheus Exporter** (same
stable Stats APIs, in-process, no dependency on the experimental telemetry
framework), but *push* instead of *pull*, and it lands data in the same
**ss4o** shape the native OTel framework is aiming for long-term — without
requiring an OTel Collector or Data Prepper hop. Best fit: teams already
using OpenSearch Dashboards for observability who don't want to stand up a
parallel Prometheus/Grafana stack just for cluster monitoring.

---

## License

Apache 2.0 — Seacom Srl
