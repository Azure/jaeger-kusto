package store

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	kustoquery "github.com/Azure/azure-kusto-go/azkustodata/query"
	"github.com/hashicorp/go-hclog"
)

// kustoV2Reader encapsulates Kusto query logic for the V2 gRPC services.
// It is storage-model agnostic — it returns raw Kusto result sets that the
// gRPC handlers convert into proto types.
type kustoV2Reader struct {
	client             kustoReaderClient
	database           string
	tableName          string
	logger             hclog.Logger
	defaultReadOptions []azkustodata.QueryOption
	cache              *discoveryCache
	depRefresher       *dependencyRefresherV2
}

func newKustoV2Reader(factory *kustoFactory, logger hclog.Logger, defaultReadOptions []azkustodata.QueryOption, cache *discoveryCache) *kustoV2Reader {
	return &kustoV2Reader{
		client:             factory.Reader(),
		database:           factory.Database,
		tableName:          factory.Table,
		logger:             logger,
		defaultReadOptions: defaultReadOptions,
		cache:              cache,
	}
}

// V2 query constants — these return the raw OTEL columns from Kusto without
// the V1 Jaeger model projections.
const (
	v2GetTraceQuery = ` | where TraceID == ParamTraceID` +
		` | project TraceID, SpanID, ParentID, SpanName, SpanKind, SpanStatus, SpanStatusMessage,` +
		` StartTime, EndTime, ResourceAttributes, TraceAttributes, Events, Links`

	v2GetServicesQuery = `| extend ProcessServiceName=tostring(ResourceAttributes.['service.name'])` +
		` | where ProcessServiceName!=""` +
		` | summarize by ProcessServiceName` +
		` | sort by ProcessServiceName asc`

	v2GetOpsNoParamsQuery = `| summarize count() by SpanName, SpanKind` +
		` | sort by count_` +
		` | project OperationName=SpanName, SpanKind`

	v2GetOpsWithServiceQuery = ` | extend ProcessServiceName=tostring(ResourceAttributes.['service.name'])` +
		` | where ProcessServiceName == ParamProcessServiceName` +
		` | summarize count() by SpanName, SpanKind` +
		` | sort by count_` +
		` | project OperationName=SpanName, SpanKind`

	v2GetTraceIdBaseQuery = ` | extend Duration=datetime_diff('microsecond',EndTime,StartTime),` +
		` ProcessServiceName=tostring(ResourceAttributes.['service.name'])`

	v2GetTracesBaseQuery = ` | extend ProcessServiceName=tostring(ResourceAttributes.['service.name']),` +
		` Duration=datetime_diff('microsecond',EndTime,StartTime)`

	v2TracesProjection = ` | project TraceID, SpanID, ParentID, SpanName, SpanKind, SpanStatus,` +
		` SpanStatusMessage, StartTime, EndTime, ResourceAttributes, TraceAttributes, Events, Links`

	v2DependenciesGraphQuery = `
	| where StartTime between ((ParamEndTs - ParamLookBack) .. ParamEndTs)
	| extend ServiceName = tostring(ResourceAttributes.['service.name'])
	| project SpanID, ParentID, ServiceName;
	spans
	| make-graph ParentID --> SpanID with spans on SpanID
	| graph-match (parent)-[]->(child)
		where parent.ServiceName != child.ServiceName
		project Parent=parent.ServiceName, Child=child.ServiceName
	| summarize CallCount=count() by Parent, Child`
)

// getTraceByID queries Kusto for all spans of a single trace.
func (r *kustoV2Reader) getTraceByID(ctx context.Context, traceID string) ([]*otlpSpan, error) {
	kustoStmt := kql.New("").AddTable(r.tableName).AddLiteral(v2GetTraceQuery)
	params := kql.NewParameters().AddString("ParamTraceID", traceID)

	clientRequestID := GetClientId()
	dataset, err := r.client.Query(ctx, r.database, kustoStmt,
		append(r.defaultReadOptions,
			azkustodata.ClientRequestID(clientRequestID),
			azkustodata.QueryParameters(params))...)
	if err != nil {
		r.logger.Error("Failed running V2 GetTrace query", "traceID", traceID, "clientRequestID", clientRequestID)
		return nil, err
	}

	return r.rowsToOTLPSpans(dataset)
}

// getServices returns all service names.
func (r *kustoV2Reader) getServices(ctx context.Context) ([]string, error) {
	const cacheKey = "services"
	if r.cache != nil {
		if cached, ok := r.cache.get(cacheKey); ok {
			r.logger.Debug("V2 GetServices: returning cached result")
			return cached.([]string), nil
		}
	}

	clientRequestID := GetClientId()
	kustoStmt := kql.New(queryResultsCacheAge).AddTable(r.tableName).AddLiteral(v2GetServicesQuery)
	dataset, err := r.client.Query(ctx, r.database, kustoStmt,
		append(r.defaultReadOptions, azkustodata.ClientRequestID(clientRequestID))...)
	if err != nil {
		r.logger.Error("Failed running V2 GetServices query", "clientRequestID", clientRequestID)
		return nil, err
	}

	type Service struct {
		ServiceName string `kusto:"ProcessServiceName"`
	}

	var services []string
	for _, row := range dataset.Tables()[0].Rows() {
		svc := Service{}
		if err := row.ToStruct(&svc); err != nil {
			return nil, err
		}
		services = append(services, svc.ServiceName)
	}

	if r.cache != nil {
		r.cache.set(cacheKey, services)
	}

	return services, nil
}

// operationResult holds a single operation row from Kusto.
type operationResult struct {
	OperationName string `kusto:"OperationName"`
	SpanKind      string `kusto:"SpanKind"`
}

// getOperations returns operations for a service.
func (r *kustoV2Reader) getOperations(ctx context.Context, service, spanKind string) ([]operationResult, error) {
	cacheKey := fmt.Sprintf("operations:%s:%s", service, spanKind)
	if r.cache != nil {
		if cached, ok := r.cache.get(cacheKey); ok {
			return cached.([]operationResult), nil
		}
	}

	clientRequestID := GetClientId()
	var dataset kustoquery.Dataset
	var err error

	if service == "" && spanKind == "" {
		kustoStmt := kql.New(queryResultsCacheAge).AddTable(r.tableName).AddLiteral(v2GetOpsNoParamsQuery)
		dataset, err = r.client.Query(ctx, r.database, kustoStmt,
			append(r.defaultReadOptions, azkustodata.ClientRequestID(clientRequestID))...)
	} else {
		kustoStmt := kql.New(queryResultsCacheAge).AddTable(r.tableName).AddLiteral(v2GetOpsWithServiceQuery)
		params := kql.NewParameters().AddString("ParamProcessServiceName", service)
		dataset, err = r.client.Query(ctx, r.database, kustoStmt,
			append(r.defaultReadOptions, azkustodata.ClientRequestID(clientRequestID), azkustodata.QueryParameters(params))...)
	}

	if err != nil {
		r.logger.Error("Failed running V2 GetOperations query", "clientRequestID", clientRequestID)
		return nil, err
	}

	var ops []operationResult
	for _, row := range dataset.Tables()[0].Rows() {
		op := operationResult{}
		if err := row.ToStruct(&op); err != nil {
			return nil, err
		}
		ops = append(ops, op)
	}

	if r.cache != nil {
		r.cache.set(cacheKey, ops)
	}

	return ops, nil
}

// traceQueryParams holds the V2 find traces query parameters.
type traceQueryParams struct {
	ServiceName   string
	OperationName string
	Attributes    map[string]string
	StartTimeMin  time.Time
	StartTimeMax  time.Time
	DurationMin   time.Duration
	DurationMax   time.Duration
	SearchDepth   int32
}

const v2DefaultNumTraces = 20

// findTraceIDs returns trace IDs matching the query.
func (r *kustoV2Reader) findTraceIDs(ctx context.Context, q *traceQueryParams) ([]string, error) {
	kustoStmt := kql.New("").AddTable(r.tableName).AddLiteral(v2GetTraceIdBaseQuery)
	params := kql.NewParameters()

	if q.ServiceName != "" {
		kustoStmt = kustoStmt.AddLiteral(` | where ProcessServiceName == ParamProcessServiceName`)
		params = params.AddString("ParamProcessServiceName", q.ServiceName)
	}

	if q.OperationName != "" {
		kustoStmt = kustoStmt.AddLiteral(` | where SpanName == ParamOperationName`)
		params = params.AddString("ParamOperationName", q.OperationName)
	}

	if q.Attributes != nil {
		for k, v := range q.Attributes {
			kustoStmt = kustoStmt.AddUnsafe(buildTagFilter(k, v))
		}
	}

	if !q.StartTimeMin.IsZero() {
		kustoStmt = kustoStmt.AddLiteral(` | where StartTime > ParamStartTimeMin`)
		params = params.AddDateTime("ParamStartTimeMin", q.StartTimeMin)
	}

	if !q.StartTimeMax.IsZero() {
		kustoStmt = kustoStmt.AddLiteral(` | where StartTime < ParamStartTimeMax`)
		params = params.AddDateTime("ParamStartTimeMax", q.StartTimeMax)
	}

	if q.DurationMin != 0 {
		kustoStmt = kustoStmt.AddLiteral(` | where datetime_diff('microsecond', EndTime, StartTime) >= ParamDurationMin`)
		params = params.AddLong("ParamDurationMin", q.DurationMin.Microseconds())
	}

	if q.DurationMax != 0 {
		kustoStmt = kustoStmt.AddLiteral(` | where datetime_diff('microsecond', EndTime, StartTime) <= ParamDurationMax`)
		params = params.AddLong("ParamDurationMax", q.DurationMax.Microseconds())
	}

	kustoStmt = kustoStmt.AddLiteral("| summarize by TraceID")

	numTraces := q.SearchDepth
	if numTraces == 0 {
		numTraces = v2DefaultNumTraces
	}
	if numTraces > 500 {
		numTraces = 500
	}
	kustoStmt = kustoStmt.AddLiteral(`| sample ParamNumTraces`)
	params = params.AddInt("ParamNumTraces", numTraces)

	r.logger.Info("V2 FindTraceIDs query", "kql", kustoStmt.String(), "service", q.ServiceName)
	clientRequestID := GetClientId()
	dataset, err := r.client.Query(ctx, r.database, kustoStmt,
		append(r.defaultReadOptions,
			azkustodata.ClientRequestID(clientRequestID),
			azkustodata.QueryParameters(params))...)
	if err != nil {
		return nil, err
	}

	type TraceIDRow struct {
		TraceID string `kusto:"TraceID"`
	}

	var ids []string
	for _, row := range dataset.Tables()[0].Rows() {
		rec := TraceIDRow{}
		if err := row.ToStruct(&rec); err != nil {
			return nil, err
		}
		ids = append(ids, rec.TraceID)
	}

	if len(ids) == 0 {
		r.logger.Warn("V2 FindTraceIDs: 0 results", "service", q.ServiceName)
	}

	return ids, nil
}

// findTraces returns full spans matching the query, grouped as otlpSpan slices.
func (r *kustoV2Reader) findTraces(ctx context.Context, q *traceQueryParams) ([]*otlpSpan, error) {
	numTraces := q.SearchDepth
	if numTraces == 0 {
		numTraces = v2DefaultNumTraces
	}
	if numTraces > 500 {
		numTraces = 500
	}

	kustoStmt := kql.New("let TraceIDs = (").AddTable(r.tableName).AddLiteral(v2GetTracesBaseQuery)
	params := kql.NewParameters()

	if q.ServiceName != "" {
		kustoStmt = kustoStmt.AddLiteral(` | where ProcessServiceName == ParamProcessServiceName`)
		params = params.AddString("ParamProcessServiceName", q.ServiceName)
	}

	if q.OperationName != "" {
		kustoStmt = kustoStmt.AddLiteral(` | where SpanName == ParamOperationName`)
		params = params.AddString("ParamOperationName", q.OperationName)
	}

	if q.Attributes != nil {
		for k, v := range q.Attributes {
			kustoStmt = kustoStmt.AddUnsafe(buildTagFilter(k, v))
		}
	}

	if !q.StartTimeMin.IsZero() {
		kustoStmt = kustoStmt.AddLiteral(` | where StartTime > ParamStartTimeMin`)
		params = params.AddDateTime("ParamStartTimeMin", q.StartTimeMin)
	}

	if !q.StartTimeMax.IsZero() {
		kustoStmt = kustoStmt.AddLiteral(` | where StartTime < ParamStartTimeMax`)
		params = params.AddDateTime("ParamStartTimeMax", q.StartTimeMax)
	}

	if q.DurationMin != 0 {
		kustoStmt = kustoStmt.AddLiteral(` | where datetime_diff('microsecond', EndTime, StartTime) >= ParamDurationMin`)
		params = params.AddLong("ParamDurationMin", q.DurationMin.Microseconds())
	}

	if q.DurationMax != 0 {
		kustoStmt = kustoStmt.AddLiteral(` | where datetime_diff('microsecond', EndTime, StartTime) <= ParamDurationMax`)
		params = params.AddLong("ParamDurationMax", q.DurationMax.Microseconds())
	}

	kustoStmt = kustoStmt.AddLiteral(" | summarize by TraceID")
	kustoStmt = kustoStmt.AddLiteral(` | sample ParamNumTraces`)
	params = params.AddInt("ParamNumTraces", numTraces)

	// Second pass: fetch all spans for matched trace IDs
	kustoStmt = kustoStmt.AddLiteral(`); `).AddTable(r.tableName).AddLiteral(v2GetTracesBaseQuery)

	if !q.StartTimeMin.IsZero() {
		kustoStmt = kustoStmt.AddLiteral(` | where StartTime > ParamStartTimeMin`)
	}
	if !q.StartTimeMax.IsZero() {
		kustoStmt = kustoStmt.AddLiteral(` | where StartTime < ParamStartTimeMax`)
	}

	kustoStmt = kustoStmt.AddLiteral(` | where TraceID in (TraceIDs)`)
	kustoStmt = kustoStmt.AddLiteral(v2TracesProjection)

	r.logger.Info("V2 FindTraces query", "kql", kustoStmt.String(), "service", q.ServiceName)
	clientRequestID := GetClientId()
	dataset, err := r.client.Query(ctx, r.database, kustoStmt,
		append(r.defaultReadOptions,
			azkustodata.ClientRequestID(clientRequestID),
			azkustodata.QueryParameters(params))...)
	if err != nil {
		return nil, err
	}

	return r.rowsToOTLPSpans(dataset)
}

// dependencyResult holds a single dependency row from Kusto.
type dependencyResult struct {
	Parent    string `kusto:"Parent"`
	Child     string `kusto:"Child"`
	CallCount int64  `kusto:"CallCount"`
}

// fetchDependencies queries Kusto for the service dependency graph.
func (r *kustoV2Reader) fetchDependencies(ctx context.Context, startTime, endTime time.Time) ([]dependencyResult, error) {
	lookback := endTime.Sub(startTime)
	if lookback > maxDependencyLookback {
		r.logger.Warn("Capping dependency lookback", "requested", lookback, "max", maxDependencyLookback)
		lookback = maxDependencyLookback
		startTime = endTime.Add(-lookback)
	}

	kustoStmt := kql.New(queryResultsCacheAge + "let spans = ").AddTable(r.tableName).AddLiteral(v2DependenciesGraphQuery)
	params := kql.NewParameters().AddDateTime("ParamEndTs", endTime).AddTimespan("ParamLookBack", lookback)
	clientRequestID := GetClientId()
	dataset, err := r.client.Query(ctx, r.database, kustoStmt,
		append(r.defaultReadOptions,
			azkustodata.ClientRequestID(clientRequestID),
			azkustodata.QueryParameters(params))...)
	if err != nil {
		return nil, err
	}

	var deps []dependencyResult
	for _, row := range dataset.Tables()[0].Rows() {
		rec := dependencyResult{}
		if err := row.ToStruct(&rec); err != nil {
			return nil, err
		}
		deps = append(deps, rec)
	}

	return deps, nil
}

// getCachedDependencies returns cached dependency results if available.
func (r *kustoV2Reader) getCachedDependencies(ctx context.Context, startTime, endTime time.Time) ([]dependencyResult, bool) {
	if r.depRefresher != nil {
		if cached, ok := r.cache.get(dependencyCacheKey); ok {
			r.logger.Debug("V2 GetDependencies: returning from cache")
			return cached.([]dependencyResult), true
		}
	}
	return nil, false
}

// rowsToOTLPSpans converts a Kusto dataset into otlpSpan structs.
func (r *kustoV2Reader) rowsToOTLPSpans(dataset kustoquery.Dataset) ([]*otlpSpan, error) {
	var spans []*otlpSpan
	for _, row := range dataset.Tables()[0].Rows() {
		rec := &otlpSpan{}
		if err := row.ToStruct(rec); err != nil {
			return nil, err
		}
		spans = append(spans, rec)
	}
	return spans, nil
}
