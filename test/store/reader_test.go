//go:build integration
// +build integration

package store

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Azure/jaeger-kusto/config"
	"github.com/Azure/jaeger-kusto/store"
	"github.com/hashicorp/go-hclog"
	"github.com/tj/assert"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

func setupKustoStore(t *testing.T) (shared.StoragePlugin, *config.KustoConfig, context.Context, *bytes.Buffer, hclog.Logger) {
	kustoConfig, err := config.ParseKustoConfig(testPluginConfig.KustoConfigPath, testPluginConfig.ReadNoTruncation, testPluginConfig.ReadNoTimeout)
	var buf bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{
		Output: &buf,
		Level:  hclog.Debug,
	})
	if err != nil {
		t.Fatalf("failed to create kusto config: %v", err)
	}
	kustoStore, err := store.NewStore(testPluginConfig, kustoConfig, logger)
	if err != nil {
		t.Fatalf("failed to create store: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return kustoStore, kustoConfig, ctx, &buf, logger
}
func TestKustoSpanReader_GetTrace(t *testing.T) {
	kustoStore, kustoConfig, ctx, buf, logger := setupKustoStore(t)
	expectedOutput := fmt.Sprintf(`%s | where TraceID == ParamTraceID | extend Duration=datetime_diff('microsecond',EndTime,StartTime) , ProcessServiceName=tostring(ResourceAttributes.['service.name']) | project-rename Tags=TraceAttributes,Logs=Events,ProcessTags=ResourceAttributes| extend References=iff(isempty(ParentID),todynamic("[]"),pack_array(bag_pack("refType","CHILD_OF","traceID",TraceID,"spanID",ParentID)))`, kustoConfig.TraceTableName)
	trace, _ := model.TraceIDFromString("3f6d8f4c5008352055c14804949d1e57")
	fulltrace, err := kustoStore.SpanReader().GetTrace(ctx, trace)
	output := buf.String()

	if !strings.Contains(output, expectedOutput) {
		t.Logf("FAILED : TestKustoSpanReader_GetTrace:  Wrong prepared query.Expected: %s, got: %s", expectedOutput, output)
		t.Fail()
	}

	if err != nil {
		logger.Error("can't get trace", err.Error())
	}
	fmt.Printf("%+v\n", fulltrace)
}

func TestKustoSpanReader_GetServices(t *testing.T) {
	kustoStore, kustoConfig, ctx, buf, logger := setupKustoStore(t)
	expectedOutput := fmt.Sprintf(`set query_results_cache_max_age = time(5m); %s | extend ProcessServiceName=tostring(ResourceAttributes.['service.name']) | where ProcessServiceName!="" | summarize by ProcessServiceName | sort by ProcessServiceName asc`, kustoConfig.TraceTableName)
	buf.Reset()
	services, err := kustoStore.SpanReader().GetServices(ctx)
	output := strings.ReplaceAll(buf.String(), "\n", "")
	if !strings.Contains(output, expectedOutput) {
		t.Logf("FAILED : TestKustoSpanReader_GetServices:  Wrong prepared query. Expected: %s, got: %s", expectedOutput, output)
		t.Fail()
	}
	if err != nil {
		logger.Error("can't get services", err.Error())
	}
	logger.Info("services", "services", services)
}

// func TestKustoSpanReader_GetOperations(t *testing.T) {
// 	kustoConfig, _ := config.ParseKustoConfig(testPluginConfig.KustoConfigPath, testPluginConfig.ReadNoTruncation, testPluginConfig.ReadNoTimeout)
// 	kustoStore, _ := store.NewStore(testPluginConfig, kustoConfig, logger)

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()

// 	operations, err := kustoStore.SpanReader().GetOperations(ctx, spanstore.OperationQueryParameters{
// 		ServiceName: "frontend",
// 		SpanKind:    "",
// 	})
// 	if err != nil {
// 		logger.Error("can't get operations", err.Error())
// 	}
// 	fmt.Printf("%+v\n", operations)
// }

// func TestFindTraces(tester *testing.T) {
// 	query := spanstore.TraceQueryParameters{
// 		ServiceName:   "my-service",
// 		OperationName: "",
// 		StartTimeMin:  time.Date(2023, time.January, 29, 06, 0, 0, 0, time.UTC),
// 		StartTimeMax:  time.Date(2023, time.January, 30, 23, 0, 0, 0, time.UTC),
// 		NumTraces:     20,
// 		Tags: map[string]string{
// 			"http_method": "GET",
// 		},
// 	}

// 	kustoConfig, _ := config.ParseKustoConfig(testPluginConfig.KustoConfigPath, testPluginConfig.ReadNoTruncation, testPluginConfig.ReadNoTimeout)
// 	kustoStore, _ := store.NewStore(testPluginConfig, kustoConfig, logger)

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()

// 	traces, err := kustoStore.SpanReader().FindTraces(ctx, &query)
// 	if err != nil {
// 		logger.Error("can't find traces", err.Error())
// 	}
// 	fmt.Printf("%+v\n", traces)
// }

func TestStore_DependencyReader(t *testing.T) {
	kustoConfig, _ := config.ParseKustoConfig(testPluginConfig.KustoConfigPath, testPluginConfig.ReadNoTruncation, testPluginConfig.ReadNoTimeout)
	kustoStore, _ := store.NewStore(testPluginConfig, kustoConfig, logger)
	dependencyLinks, err := kustoStore.DependencyReader().GetDependencies(context.Background(), time.Now(), 168*time.Hour)
	if err != nil {
		logger.Error("can't find dependencyLinks", err.Error())
	}
	fmt.Printf("%+v\n", dependencyLinks)
}

func TestFindTracesWithDurationMaxVerification(t *testing.T) {
	query := &spanstore.TraceQueryParameters{
		ServiceName:  "test-service",
		StartTimeMin: time.Date(2023, time.January, 29, 06, 0, 0, 0, time.UTC),
		StartTimeMax: time.Date(2023, time.January, 30, 23, 0, 0, 0, time.UTC),
		DurationMax:  time.Millisecond * 500,
		NumTraces:    10,
	}

	kustoConfig := &config.KustoConfig{
		ClientID:       "test-client-id",
		ClientSecret:   "test-client-secret",
		TenantID:       "test-tenant-id",
		Endpoint:       "https://test.kusto.windows.net",
		Database:       "test-db",
		TraceTableName: "OTELTraces",
	}

	var buf bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{
		Output: &buf,
		Level:  hclog.Debug,
	})

	kustoStore, err := store.NewStore(testPluginConfig, kustoConfig, logger)
	if err != nil {
		t.Skipf("Skipping test due to store creation error: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = kustoStore.SpanReader().FindTraces(ctx, query)

	output := buf.String()

	assert.Contains(t, output, "| where Duration < ParamDurationMax",
		"FindTraces should generate correct duration max condition with '<' operator")

	assert.NotContains(t, output, "| where Duration > ParamDurationMax",
		"FindTraces should not generate incorrect duration max condition with '>' operator")
}

func TestFindTracesWithDurationMax(t *testing.T) {
	query := &spanstore.TraceQueryParameters{
		ServiceName:  "test-service",
		StartTimeMin: time.Date(2023, time.January, 29, 06, 0, 0, 0, time.UTC),
		StartTimeMax: time.Date(2023, time.January, 30, 23, 0, 0, 0, time.UTC),
		DurationMax:  time.Millisecond * 500,
		NumTraces:    10,
	}

	kustoConfig := &config.KustoConfig{
		ClientID:       "test-client-id",
		ClientSecret:   "test-client-secret",
		TenantID:       "test-tenant-id",
		Endpoint:       "https://test.kusto.windows.net",
		Database:       "test-db",
		TraceTableName: "OTELTraces",
	}

	var buf bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{
		Output: &buf,
		Level:  hclog.Debug,
	})

	kustoStore, err := store.NewStore(testPluginConfig, kustoConfig, logger)
	if err != nil {
		t.Skipf("Skipping test due to store creation error: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = kustoStore.SpanReader().FindTraces(ctx, query)

	output := buf.String()

	assert.Contains(t, output, "| where Duration < ParamDurationMax",
		"FindTraces should generate correct duration max condition with '<' operator")

	assert.NotContains(t, output, "| where Duration > ParamDurationMax",
		"FindTraces should not generate incorrect duration max condition with '>' operator")
}

func TestFindTracesWithBothDurationMinAndMax(t *testing.T) {
	query := &spanstore.TraceQueryParameters{
		ServiceName:  "test-service",
		StartTimeMin: time.Date(2023, time.January, 29, 06, 0, 0, 0, time.UTC),
		StartTimeMax: time.Date(2023, time.January, 30, 23, 0, 0, 0, time.UTC),
		DurationMin:  time.Millisecond * 100,
		DurationMax:  time.Millisecond * 500,
		NumTraces:    10,
	}

	kustoConfig := &config.KustoConfig{
		ClientID:       "test-client-id",
		ClientSecret:   "test-client-secret",
		TenantID:       "test-tenant-id",
		Endpoint:       "https://test.kusto.windows.net",
		Database:       "test-db",
		TraceTableName: "OTELTraces",
	}

	var buf bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{
		Output: &buf,
		Level:  hclog.Debug,
	})

	kustoStore, err := store.NewStore(testPluginConfig, kustoConfig, logger)
	if err != nil {
		t.Skipf("Skipping test due to store creation error: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = kustoStore.SpanReader().FindTraces(ctx, query)

	output := buf.String()

	assert.Contains(t, output, "| where Duration > ParamDurationMin",
		"FindTraces should generate correct duration min condition with '>' operator")
	assert.Contains(t, output, "| where Duration < ParamDurationMax",
		"FindTraces should generate correct duration max condition with '<' operator")

	assert.NotContains(t, output, "| where Duration < ParamDurationMin",
		"FindTraces should not generate incorrect duration min condition")
	assert.NotContains(t, output, "| where Duration > ParamDurationMax",
		"FindTraces should not generate incorrect duration max condition")
}

func TestFindTraceIDsWithDurationMax(t *testing.T) {
	query := &spanstore.TraceQueryParameters{
		ServiceName:  "test-service",
		StartTimeMin: time.Date(2023, time.January, 29, 06, 0, 0, 0, time.UTC),
		StartTimeMax: time.Date(2023, time.January, 30, 23, 0, 0, 0, time.UTC),
		DurationMax:  time.Millisecond * 500,
		NumTraces:    10,
	}

	kustoConfig := &config.KustoConfig{
		ClientID:       "test-client-id",
		ClientSecret:   "test-client-secret",
		TenantID:       "test-tenant-id",
		Endpoint:       "https://test.kusto.windows.net",
		Database:       "test-db",
		TraceTableName: "OTELTraces",
	}

	var buf bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{
		Output: &buf,
		Level:  hclog.Debug,
	})

	kustoStore, err := store.NewStore(testPluginConfig, kustoConfig, logger)
	if err != nil {
		t.Skipf("Skipping test due to store creation error: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = kustoStore.SpanReader().FindTraceIDs(ctx, query)

	output := buf.String()

	assert.Contains(t, output, "| where Duration < ParamDurationMax",
		"FindTraceIDs should generate correct duration max condition with '<' operator")

	assert.NotContains(t, output, "| where Duration > ParamDurationMax",
		"FindTraceIDs should not generate incorrect duration max condition with '>' operator")
}

func TestFindTraceIDsWithDurationMin(t *testing.T) {
	query := &spanstore.TraceQueryParameters{
		ServiceName:  "test-service",
		StartTimeMin: time.Date(2023, time.January, 29, 06, 0, 0, 0, time.UTC),
		StartTimeMax: time.Date(2023, time.January, 30, 23, 0, 0, 0, time.UTC),
		DurationMin:  time.Millisecond * 100,
		NumTraces:    10,
	}

	kustoConfig := &config.KustoConfig{
		ClientID:       "test-client-id",
		ClientSecret:   "test-client-secret",
		TenantID:       "test-tenant-id",
		Endpoint:       "https://test.kusto.windows.net",
		Database:       "test-db",
		TraceTableName: "OTELTraces",
	}

	var buf bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{
		Output: &buf,
		Level:  hclog.Debug,
	})

	kustoStore, err := store.NewStore(testPluginConfig, kustoConfig, logger)
	if err != nil {
		t.Skipf("Skipping test due to store creation error: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = kustoStore.SpanReader().FindTraceIDs(ctx, query)

	output := buf.String()

	assert.Contains(t, output, "| where Duration > ParamDurationMin",
		"FindTraceIDs should generate correct duration min condition with '>' operator")
}

func TestFindTraceIDsWithBothDurationMinAndMax(t *testing.T) {
	query := &spanstore.TraceQueryParameters{
		ServiceName:  "test-service",
		StartTimeMin: time.Date(2023, time.January, 29, 06, 0, 0, 0, time.UTC),
		StartTimeMax: time.Date(2023, time.January, 30, 23, 0, 0, 0, time.UTC),
		DurationMin:  time.Millisecond * 100,
		DurationMax:  time.Millisecond * 500,
		NumTraces:    10,
	}

	kustoConfig := &config.KustoConfig{
		ClientID:       "test-client-id",
		ClientSecret:   "test-client-secret",
		TenantID:       "test-tenant-id",
		Endpoint:       "https://test.kusto.windows.net",
		Database:       "test-db",
		TraceTableName: "OTELTraces",
	}

	var buf bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{
		Output: &buf,
		Level:  hclog.Debug,
	})

	kustoStore, err := store.NewStore(testPluginConfig, kustoConfig, logger)
	if err != nil {
		t.Skipf("Skipping test due to store creation error: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _ = kustoStore.SpanReader().FindTraceIDs(ctx, query)

	output := buf.String()

	assert.Contains(t, output, "| where Duration > ParamDurationMin",
		"FindTraceIDs should generate correct duration min condition with '>' operator")
	assert.Contains(t, output, "| where Duration < ParamDurationMax",
		"FindTraceIDs should generate correct duration max condition with '<' operator")

	assert.NotContains(t, output, "| where Duration < ParamDurationMin",
		"FindTraceIDs should not generate incorrect duration min condition")
	assert.NotContains(t, output, "| where Duration > ParamDurationMax",
		"FindTraceIDs should not generate incorrect duration max condition")
}
