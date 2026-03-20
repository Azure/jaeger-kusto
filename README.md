# Azure Data Explorer (Kusto) gRPC backend for Jaeger

This is a **Jaeger V2 remote storage** gRPC backend for [Jaeger](https://www.jaegertracing.io/) that reads traces from **Azure Data Explorer (Kusto)**. Originally forked from https://github.com/dodopizza/jaeger-kusto, it now supports the OTEL exporter used with ADX and has been migrated to Jaeger V2's gRPC Remote Storage API with OTLP-native data models.

> **Note:** This plugin is read-only. Trace ingestion is handled by the [OpenTelemetry Collector's ADX exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/azuredataexplorerexporter/README.md). The plugin only queries Kusto to render traces in the Jaeger UI.

## Architecture

```
OTEL Collector → ADX Exporter → Kusto (OTELTraces table)
                                        ↓ (read)
Jaeger V2 ←── gRPC Remote Storage ←── jaeger-kusto plugin
   ↑                                        ↓
   └────── PromQL (metrics shim) ←── metrics/ package
```

The plugin implements three gRPC services:
- **TraceReader** — GetTraces, GetServices, GetOperations, FindTraces, FindTraceIDs
- **DependencyReader** — GetDependencies (service graph)
- **TraceService** — OTEL Export (no-op, since ingestion is handled externally)

## Installation and testing

For local testing, you need Docker and docker-compose.

First, you need an Azure Data Explorer cluster: <https://docs.microsoft.com/en-us/azure/data-explorer/create-cluster-database-portal>

Then, set up the Kusto/ADX exporter tables as explained in the [OTEL ADX exporter docs](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/exporter/azuredataexplorerexporter/README.md).

## Authentication

The plugin uses a JSON config file for Kusto authentication:

```json
{
  "clientId": "",
  "clientSecret": "",
  "database": "<database>",
  "endpoint": "https://<cluster>.<region>.kusto.windows.net",
  "tenantId": "",
  "traceTableName": "<trace_table>",
  "useManagedIdentity": false,
  "useWorkloadIdentity": false
}
```

| Auth Method | Description |
| --- | --- |
| **AAD App Key** | Set `clientId`, `clientSecret`, `tenantId` |
| **Managed Identity** | Set `useManagedIdentity: true`, use `clientId` for the managed identity's client ID |
| **Workload Identity** | Set `useWorkloadIdentity: true` — uses Azure default credentials (environment variables → workload identity) |

Save this file as `jaeger-kusto-config.json` in the root of the repository.

## Local runs

Start with docker-compose:

```bash
docker compose -f build/server/docker-compose.yml up --build
```

This starts:
- **jaeger-kusto plugin** on port 8989 (gRPC) and 9090 (metrics shim)
- **Jaeger V2** on port 16686 (UI), configured to use the plugin as remote storage

Open <http://localhost:16686> for the Jaeger UI.

## Deploying to Kubernetes

Deploy using the provided Helm chart:

```bash
helm install jaeger-kusto build/server/helm/ -n <namespace>
helm upgrade jaeger-kusto build/server/helm/ -n <namespace>
```

### Configuration Properties

| Property | Description | Default |
| --- | --- | --- |
| `baseConfig.logLevel` | Log level for the plugin | `info` |
| `baseConfig.logJson` | JSON log format | `false` |
| `baseConfig.readNoTruncation` | Bypass [Kusto query limits](https://aka.ms/kustoquerylimits) | `false` |
| `baseConfig.readNoTimeout` | Extend default 10min query timeout | `false` |
| `authConfig.clientId` | AAD App ID or Managed Identity client ID | `""` |
| `authConfig.clientSecret` | AAD App Secret | `""` |
| `authConfig.tenantId` | AAD tenant ID | `""` |
| `authConfig.useManagedIdentity` | Use managed identity auth | `false` |
| `authConfig.useWorkloadIdentity` | Use workload identity auth | `false` |
| `authConfig.database` | Kusto database name | `""` |
| `authConfig.clusterUrl` | Kusto cluster URL | `""` |
| `authConfig.traceTableName` | Trace table name | `"OTELTraces"` |
| `authConfig.metricsViewName` | Materialized view for RED metrics | `""` |
| `image.repository` | Plugin image repository | `sdkdemosacr.azurecr.io/jaeger-kusto` |
| `image.tag` | Plugin image tag | `"2.0.0-Preview"` |
| `image.pullPolicy` | Image pull policy | `"Always"` |
| `jaeger.image` | Jaeger image | `"jaegertracing/jaeger"` |
| `jaeger.imageTag` | Jaeger image tag | `"2"` |
| `metrics.enabled` | Enable RED metrics / SPM | `true` |




## Known Limitations

* Tag-based search is supported for span attributes and resource attributes
* The TraceService (write path) is a no-op — ingestion must be handled by the OTEL Collector's ADX exporter


## RED Metrics / Service Performance Monitoring (SPM)

The plugin supports [Jaeger's SPM (Service Performance Monitoring)](https://www.jaegertracing.io/docs/2.dev/architecture/spm/) feature, which surfaces RED metrics (Rate, Errors, Duration) in the Jaeger UI "Monitor" tab.

### Architecture

Instead of requiring Prometheus or a separate metrics pipeline, this plugin computes RED metrics directly from your OTELTraces data in Kusto using a **Materialized View** for pre-aggregation, and exposes them via a **PromQL-compatible HTTP API** (shim) that Jaeger V2's built-in prometheus metric backend can query.

```
OTELTraces table → SpanMetrics Materialized View → PromQL Shim (built-in) → Jaeger V2
```

### Setup

#### 1. Create the Kusto Materialized View

Run the KQL script in `config/kusto-materialized-view.kql` against your Kusto database:

```kql
.create-or-alter async materialized-view with (backfill=true) SpanMetrics on table OTELTraces
{
    OTELTraces
    | extend
        ServiceName = tostring(ResourceAttributes.['service.name']),
        Duration_ms = datetime_diff('millisecond', EndTime, StartTime),
        StatusCode = tostring(SpanStatus)
    | summarize
        call_count = count(),
        error_count = countif(StatusCode == 'STATUS_CODE_ERROR'),
        duration_sum_ms = sum(Duration_ms),
        p50_ms = percentile(Duration_ms, 50),
        p75_ms = percentile(Duration_ms, 75),
        p95_ms = percentile(Duration_ms, 95),
        p99_ms = percentile(Duration_ms, 99)
      by ServiceName, SpanName, SpanKind, bin(StartTime, 1m)
}
```

#### 2. Configure the Plugin

Add these fields to your plugin configuration JSON:

```json
{
    "metricsEnabled": true,
    "metricsListenAddress": ":9090"
}
```

And add the materialized view name to your Kusto configuration JSON:

```json
{
    "metricsViewName": "SpanMetrics"
}
```

If `metricsViewName` is empty, the plugin will query the raw `OTELTraces` table directly (slower for large datasets but requires no materialized view setup).

A full example is at `build/server/jaeger-kusto-plugin-config.json`.

#### 3. Configure Jaeger V2

Use the sample configuration at `config/jaeger-v2-config.yaml`:

```yaml
extensions:
  jaeger_storage:
    backends:
      kusto_traces:
        grpc:
          endpoint: "jaeger-kusto:8989"
          tls:
            insecure: true
          writer:
            endpoint: "jaeger-kusto:8989"
            tls:
              insecure: true
    metric_backends:
      kusto_metrics:
        prometheus:
          endpoint: "http://jaeger-kusto:9090"
  jaeger_query:
    storage:
      traces: kusto_traces
      metrics: kusto_metrics
    ui:
      config_file: /etc/jaeger/jaeger-ui.json
```

Ensure `jaeger-ui.json` has the Monitor tab enabled:

```json
{
  "monitor": {
    "menuEnabled": true
  }
}
```

### Configuration Reference

| Property | Description | Default |
| --- | --- | --- |
| `metricsEnabled` | Enable the PromQL shim server for RED metrics | `false` |
| `metricsListenAddress` | Listen address for the PromQL shim HTTP server | `":9090"` |
| `metricsViewName` | Name of the Kusto materialized view for pre-computed metrics | `""` (uses raw trace table) |


## Reporting issues

The logging is controlled in the `jaeger-kusto-plugin-config.json` in the build/server folder. Please change the logLevel to `debug` to get more detailed logs. This should show the executed query, please execute this query in Kusto and provide the payload as well to debug issues in the applied transformation. Attach both the logs and the payload to troubleshoot the issue.