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
	_, err := kustoStore.SpanReader().GetTrace(ctx, trace)
	output := buf.String()

	if !strings.Contains(output, expectedOutput) {
		t.Logf("FAILED : TestKustoSpanReader_GetTrace:  Wrong prepared query.Expected: %s, got: %s", expectedOutput, output)
		t.Fail()
	}

	if err != nil {
		logger.Error("can't get trace", err.Error())
	}
}

func TestKustoSpanReader_GetServices(t *testing.T) {
	kustoStore, kustoConfig, ctx, buf, logger := setupKustoStore(t)
	expectedOutput := fmt.Sprintf(`set query_results_cache_max_age = time(5m);%s| extend ProcessServiceName=tostring(ResourceAttributes.['service.name']) | where ProcessServiceName!="" | summarize by ProcessServiceName 	| sort by ProcessServiceName asc`, kustoConfig.TraceTableName)
	buf.Reset()
	_, err := kustoStore.SpanReader().GetServices(ctx)
	output := strings.ReplaceAll(buf.String(), "\n", "")
	if !strings.Contains(output, expectedOutput) {
		t.Logf("FAILED : TestKustoSpanReader_GetServices:  Wrong prepared query. Expected: %s, got: %s", expectedOutput, output)
		t.Fail()
	}
	if err != nil {
		logger.Error("can't get services", err.Error())
		t.Logf("FAILED : TestKustoSpanReader_GetServices:  Error: %s", err.Error())
		t.Fail()
	}
}

func TestKustoSpanReader_GetOperations(t *testing.T) {
	kustoStore, kustoConfig, ctx, buf, logger := setupKustoStore(t)
	expectedOutput := fmt.Sprintf(`set query_results_cache_max_age = time(5m);%s | extend ProcessServiceName=tostring(ResourceAttributes.['service.name']) | where ProcessServiceName == ParamProcessServiceName | summarize count() by SpanName , SpanKind | sort by count_ | project OperationName=SpanName,SpanKind`, kustoConfig.TraceTableName)

	buf.Reset()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	_, err := kustoStore.SpanReader().GetOperations(ctx, spanstore.OperationQueryParameters{
		ServiceName: "frontend",
		SpanKind:    "",
	})
	if err != nil {
		logger.Error("can't get operations", err.Error())
		t.Logf("FAILED : TestKustoSpanReader_GetOperations:  Error: %s", err.Error())
		t.Fail()
	}
	output := strings.ReplaceAll(buf.String(), "\n", "")
	if !strings.Contains(output, expectedOutput) {
		t.Logf("FAILED : TestKustoSpanReader_GetOperations:  Wrong prepared query. Expected: %s, got: %s", expectedOutput, output)
		t.Fail()
	}
}

func TestFindTraces(t *testing.T) {
	kustoStore, kustoConfig, ctx, buf, logger := setupKustoStore(t)
	// Reset the buffer for a clean output
	expectedOutput := fmt.Sprintf(`let TraceIDs = (%s | extend ProcessServiceName=tostring(ResourceAttributes.['service.name']),Duration=datetime_diff('microsecond',EndTime,StartTime) | where ProcessServiceName == ParamProcessServiceName | where TraceAttributes['http_method'] == 'GET' or ResourceAttributes['http_method'] == 'GET' | where StartTime > ParamStartTimeMin | where StartTime < ParamStartTimeMax | summarize by TraceID | sample ParamNumTraces); %s | extend ProcessServiceName=tostring(ResourceAttributes.['service.name']),Duration=datetime_diff('microsecond',EndTime,StartTime) | where StartTime > ParamStartTimeMin | where StartTime < ParamStartTimeMax | where TraceID in (TraceIDs) | project-rename Tags=TraceAttributes,Logs=Events,ProcessTags=ResourceAttributes|extend References=iff(isempty(ParentID),todynamic("[]"),pack_array(bag_pack("refType","CHILD_OF","traceID",TraceID,"spanID",ParentID)))`, kustoConfig.TraceTableName, kustoConfig.TraceTableName)
	buf.Reset()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Example query parameters
	query := spanstore.TraceQueryParameters{
		ServiceName:   "my-service",
		OperationName: "",
		StartTimeMin:  time.Date(2023, time.January, 29, 06, 0, 0, 0, time.UTC),
		StartTimeMax:  time.Date(2023, time.January, 30, 23, 0, 0, 0, time.UTC),
		NumTraces:     20,
		Tags: map[string]string{
			"http_method": "GET",
		},
	}
	// Find traces using the Kusto store
	_, err := kustoStore.SpanReader().FindTraces(ctx, &query)
	if err != nil {
		logger.Error("can't find traces", err.Error())
		t.Logf("FAILED : TestKustoSpanReader_FindTraces:  Error: %s", err.Error())
		t.Fail()
	}
	output := strings.ReplaceAll(buf.String(), "\n", "")

	if !strings.Contains(output, expectedOutput) {
		t.Logf("FAILED : TestKustoSpanReader_FindTraces:  Wrong prepared query. Expected: %s, got: %s", expectedOutput, output)
		t.Fail()
	}

}

func TestStore_DependencyReader(t *testing.T) {
	kustoStore, kustoConfig, ctx, buf, logger := setupKustoStore(t)
	expectedOutput := fmt.Sprintf(`set query_results_cache_max_age = time(5m);%s | extend ProcessServiceName=tostring(ResourceAttributes.['service.name']) | where StartTime < ParamEndTs and StartTime > (ParamEndTs-ParamLookBack) | project ProcessServiceName, SpanID, ChildOfSpanId = ParentID | join (%s | extend ProcessServiceName=tostring(ResourceAttributes.['service.name']) | project ChildOfSpanId=SpanID, ParentService=ProcessServiceName) on ChildOfSpanId | where ProcessServiceName != ParentService | extend Call=pack('Parent', ParentService, 'Child', ProcessServiceName) | summarize CallCount=count() by tostring(Call) | extend Call=parse_json(Call) | evaluate bag_unpack(Call)`, kustoConfig.TraceTableName, kustoConfig.TraceTableName)
	buf.Reset()
	_, err := kustoStore.DependencyReader().GetDependencies(ctx, time.Now(), 168*time.Hour)
	if err != nil {
		logger.Error("can't find traces", err.Error())
		t.Logf("FAILED : TestKustoSpanReader_FindTraces:  Error: %s", err.Error())
		t.Fail()
	}
	output := strings.ReplaceAll(buf.String(), "\n", "")

	if !strings.Contains(output, expectedOutput) {
		t.Logf("FAILED : TestKustoSpanReader_FindTraces:  Wrong prepared query. Expected: %s, got: %s", expectedOutput, output)
		t.Fail()
	}
}
