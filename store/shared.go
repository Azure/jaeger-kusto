package store

import (
	"context"
	"fmt"

	"github.com/Azure/azure-kusto-go/azkustodata"
	kustoquery "github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/google/uuid"
)

// kustoReaderClient abstracts the Kusto query interface for testability.
type kustoReaderClient interface {
	Query(ctx context.Context, db string, query azkustodata.Statement, options ...azkustodata.QueryOption) (kustoquery.Dataset, error)
}

// GetClientId generates a unique client request ID for Kusto query tracing.
func GetClientId() string {
	return fmt.Sprintf("azure-kusto-jaeger-%s", uuid.New().String())
}

// otelStatusCodeToKusto maps Jaeger-style otel.status_code values to the raw SpanStatus column values in Kusto.
var otelStatusCodeToKusto = map[string]string{
	"ERROR": "STATUS_CODE_ERROR",
	"OK":    "STATUS_CODE_OK",
	"UNSET": "STATUS_CODE_UNSET",
}

// spanKindToKusto maps Jaeger-style span.kind values to the raw SpanKind column values in Kusto.
var spanKindToKustoMap = map[string]string{
	"server":   "SPAN_KIND_SERVER",
	"client":   "SPAN_KIND_CLIENT",
	"consumer": "SPAN_KIND_CONSUMER",
	"producer": "SPAN_KIND_PRODUCER",
	"internal": "SPAN_KIND_INTERNAL",
}

// buildTagFilter returns a KQL filter clause for a tag key/value pair.
// Well-known synthetic tags (otel.status_code, error, span.kind) are mapped to native Kusto columns
// in addition to TraceAttributes/ResourceAttributes, since they may not be stored as span attributes.
func buildTagFilter(k, v string) string {
	switch k {
	case "otel.status_code":
		if kustoVal, ok := otelStatusCodeToKusto[v]; ok {
			return fmt.Sprintf(" | where SpanStatus == '%s' or TraceAttributes['%s'] == '%s' or ResourceAttributes['%s'] == '%s'", kustoVal, k, v, k, v)
		}
		return fmt.Sprintf(" | where TraceAttributes['%s'] == '%s' or ResourceAttributes['%s'] == '%s'", k, v, k, v)
	case "error":
		if v == "true" {
			return fmt.Sprintf(" | where SpanStatus == 'STATUS_CODE_ERROR' or TraceAttributes['%s'] == '%s' or ResourceAttributes['%s'] == '%s'", k, v, k, v)
		}
		return fmt.Sprintf(" | where TraceAttributes['%s'] == '%s' or ResourceAttributes['%s'] == '%s'", k, v, k, v)
	case "span.kind":
		if kustoVal, ok := spanKindToKustoMap[v]; ok {
			return fmt.Sprintf(" | where SpanKind == '%s' or TraceAttributes['%s'] == '%s' or ResourceAttributes['%s'] == '%s'", kustoVal, k, v, k, v)
		}
		return fmt.Sprintf(" | where TraceAttributes['%s'] == '%s' or ResourceAttributes['%s'] == '%s'", k, v, k, v)
	default:
		return fmt.Sprintf(" | where TraceAttributes['%s'] == '%s' or ResourceAttributes['%s'] == '%s'", k, v, k, v)
	}
}
