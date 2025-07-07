package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/errors"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/azure-kusto-go/kusto/kql"
	"github.com/Azure/jaeger-kusto/config"
	"github.com/Azure/jaeger-kusto/store"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	// Test data for OTEL traces
	testOTELTracesData = `3f6d8f4c5008352055c14804949d1e57,abc123def456,http-get-request,CLIENT,2024-01-01T10:00:00Z,2024-01-01T10:00:01Z,"","{""service.name"":""frontend-service"",""service.version"":""1.0.0""}","{""http.method"":""GET"",""http.url"":""/api/users""}","[]"
4a7e9f5d6119463166d25915a5a2f968,def456ghi789,database-query,SERVER,2024-01-01T10:00:00Z,2024-01-01T10:00:02Z,abc123def456,"{""service.name"":""backend-service"",""service.version"":""2.1.0""}","{""db.statement"":""SELECT * FROM users""}","[]"
5b8fa06e722a574277e3696ba6b3c079,ghi789jkl012,cache-lookup,CLIENT,2024-01-01T10:00:00Z,2024-01-01T10:00:00.5Z,"","{""service.name"":""cache-service"",""service.version"":""1.2.0""}","{""cache.key"":""user:123""}","[]"
6c9ab17f833b685388f4797cab4d118a,jkl012mno345,notification-send,PRODUCER,2024-01-01T10:00:00Z,2024-01-01T10:00:03Z,"","{""service.name"":""notification-service"",""service.version"":""1.5.0""}","{""notification.type"":""email""}","[]"
7d1bc28a944c796499a589adbcde2299,mno345pqr678,invalid-span,INTERNAL,2024-01-01T10:00:00Z,2024-01-01T10:00:01Z,"","{""service.version"":""1.0.0""}","{}","[]"`
)

func createClient(clusterUrl string) (*kusto.Client, error) {
	kcsb := kusto.NewConnectionStringBuilder(clusterUrl).WithDefaultAzureCredential()
	kcsb.SetConnectorDetails("TestJaeger", "1.0.0", "plugin", "", false, "")
	return kusto.New(kcsb)
}

func TestGetServices_Integration(t *testing.T) {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// a) Set up the test by getting environment variables for cluster, database and login using Default azure credentials
	cluster := os.Getenv("KUSTO_CLUSTER")
	database := os.Getenv("KUSTO_DATABASE")
	tenantID := os.Getenv("KUSTO_TENANT_ID")

	if cluster == "" || database == "" || tenantID == "" {
		t.Skip("Integration test requires KUSTO_CLUSTER, KUSTO_DATABASE, and KUSTO_TENANT_ID environment variables")
	}

	// Create admin client for management operations
	adminClient, err := createClient(cluster)
	require.NoError(t, err, "Failed to create admin client")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// b) Create a temp table for OTELTraces, load some data
	tempTableName := fmt.Sprintf("OTELTraces_Test_%d", time.Now().Unix())

	// Create temporary table schema using management command
	createTableCmd := fmt.Sprintf(`
        .create-merge table %s (
            TraceID: string,
            SpanID: string,
            SpanName: string,
            SpanKind: string,
            StartTime: datetime,
            EndTime: datetime,
            ParentID: string,
            ResourceAttributes: dynamic,
            TraceAttributes: dynamic,
            Events: dynamic
        )
    `, tempTableName)

	createStmt := kql.New("").AddUnsafe(createTableCmd)
	// Create the temporary table using management command
	_, err = adminClient.Mgmt(ctx, database, createStmt)
	require.NoError(t, err, "Failed to create temporary table")

	// Clean up the table after test
	defer func() {
		dropTableCmd := fmt.Sprintf(".drop table %s", tempTableName)
		_, _ = adminClient.Mgmt(ctx, database, kql.New("").AddUnsafe(dropTableCmd))
	}()

	// Load test data into the temporary table
	ingestCmd := fmt.Sprintf(`.ingest inline into table %s <| %s`, tempTableName, testOTELTracesData)

	_, err = adminClient.Mgmt(ctx, database, kql.New("").AddUnsafe(ingestCmd))
	require.NoError(t, err, "Failed to ingest test data")

	// Wait for data to be available
	time.Sleep(10 * time.Second)

	// Create kusto config using default Azure credentials
	kustoConfig := &config.KustoConfig{
		Endpoint:            cluster,
		Database:            database,
		TenantID:            tenantID,
		UseWorkloadIdentity: true,          // Use default Azure credentials
		TraceTableName:      tempTableName, // Use temp table
	}

	// Create plugin config
	pluginConfig := &config.PluginConfig{
		LogLevel: "debug",
	}

	// Create logger
	logger := hclog.New(&hclog.LoggerOptions{
		Level: hclog.Debug,
	})

	// Create store with temp table
	tempKustoStore, err := store.NewStore(pluginConfig, kustoConfig, logger)
	require.NoError(t, err, "Failed to create temp kusto store")

	// c) Run the GetServices service with the filters. Add multiple conditions for predicates
	t.Run("GetServices_AllServices", func(t *testing.T) {
		services, err := tempKustoStore.SpanReader().GetServices(ctx)
		require.NoError(t, err, "Failed to get services")

		// d) Assert the results are as expected
		expectedServices := []string{
			"backend-service",
			"cache-service",
			"frontend-service",
			"notification-service",
		}

		assert.Equal(t, len(expectedServices), len(services), "Number of services should match")

		for _, expectedService := range expectedServices {
			assert.Contains(t, services, expectedService, "Service should be present in results")
		}

		// Services should be sorted alphabetically
		for i := 1; i < len(services); i++ {
			assert.LessOrEqual(t, services[i-1], services[i], "Services should be sorted alphabetically")
		}
	})

	t.Run("GetServices_WithTimeFilter", func(t *testing.T) {
		// Create a custom query to test time-based filtering
		// This simulates filtering by time range as a predicate condition
		timeFilterQuery := fmt.Sprintf(`
            set query_results_cache_max_age = time(5m);
            %s
            | where StartTime >= datetime(2024-01-01T10:00:00Z) and StartTime <= datetime(2024-01-01T10:00:02Z)
            | extend ProcessServiceName=tostring(ResourceAttributes.['service.name'])
            | where ProcessServiceName!=""
            | summarize by ProcessServiceName
            | sort by ProcessServiceName asc
        `, tempTableName)

		iter, err := adminClient.Query(ctx, database, kql.New("").AddUnsafe(timeFilterQuery))
		require.NoError(t, err, "Failed to execute time filter query")
		defer iter.Stop()

		var services []string
		err = iter.DoOnRowOrError(func(row *table.Row, e *errors.Error) error {
			if e != nil {
				return e
			}
			var service string
			if err := row.ToStruct(&struct {
				ProcessServiceName string `kusto:"ProcessServiceName"`
			}{ProcessServiceName: service}); err != nil {
				return err
			}
			services = append(services, service)
			return nil
		})
		require.NoError(t, err, "Failed to process time filter results")

		// All services should be present as they all have timestamps within the range
		expectedServices := []string{
			"backend-service",
			"cache-service",
			"frontend-service",
			"notification-service",
		}
		assert.Equal(t, len(expectedServices), len(services), "Time filter should return all services")
	})
}
