package store

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/kql"
	"github.com/Azure/jaeger-kusto/config"
	"github.com/Azure/jaeger-kusto/store"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/spanstore"
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

var (
	kustoClient *kusto.Client
	clientOnce  sync.Once
	clientErr   error
)

// TestEnvironment holds the common test environment setup
type TestEnvironment struct {
	AdminClient   *kusto.Client
	Database      string
	TenantID      string
	Cluster       string
	TempTableName string
	KustoStore    shared.StoragePlugin
	Context       context.Context
	Cancel        context.CancelFunc
	Logger        hclog.Logger
}

func createClient(clusterUrl string) (*kusto.Client, error) {
	clientOnce.Do(func() {
		kcsb := kusto.NewConnectionStringBuilder(clusterUrl).WithDefaultAzureCredential()
		kcsb.SetConnectorDetails("TestJaeger", "1.0.0", "plugin", "", false, "")

		// Create a new Kusto client with the connection string builder
		kustoClient, clientErr = kusto.New(kcsb)
	})

	return kustoClient, clientErr
}

// setupTestEnvironment creates a common test environment for integration tests
func setupTestEnvironment(t *testing.T) *TestEnvironment {
	// Skip if not running integration tests
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Create logger
	logger := hclog.New(&hclog.LoggerOptions{
		Level: hclog.Debug,
	})

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

	// b) Create a temp table for OTELTraces, load some data
	tempTableName := fmt.Sprintf("OTELTraces_Test_%d", time.Now().Unix())

	env := &TestEnvironment{
		AdminClient:   adminClient,
		Database:      database,
		TenantID:      tenantID,
		Cluster:       cluster,
		TempTableName: tempTableName,
		Context:       ctx,
		Cancel:        cancel,
		Logger:        logger,
	}

	// Setup cleanup
	t.Cleanup(func() {
		env.Cleanup()
	})

	return env
}

// CreateTempTable creates a temporary table with the OTEL traces schema
func (env *TestEnvironment) CreateTempTable(t *testing.T) {
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
    `, env.TempTableName)

	createStmt := kql.New("").AddUnsafe(createTableCmd)
	// Create the temporary table using management command
	_, err := env.AdminClient.Mgmt(env.Context, env.Database, createStmt)
	env.Logger.Info("Created temporary table", "tableName", env.TempTableName)
	require.NoError(t, err, "Failed to create temporary table")
}

// IngestTestData loads the test data into the temporary table
func (env *TestEnvironment) IngestTestData(t *testing.T) {
	// Load test data into the temporary table
	ingestCmd := fmt.Sprintf(`.ingest inline into table %s <| %s`, env.TempTableName, testOTELTracesData)
	// Ingest the test data into the temporary table
	env.Logger.Info("Ingesting test data into temporary table", "tableName", env.TempTableName)
	_, err := env.AdminClient.Mgmt(env.Context, env.Database, kql.New("").AddUnsafe(ingestCmd))
	require.NoError(t, err, "Failed to ingest test data")

	// Wait for data to be available
	time.Sleep(10 * time.Second)
}

// CreateKustoStore creates a Kusto store configured to use the temporary table
func (env *TestEnvironment) CreateKustoStore(t *testing.T) {
	// Create kusto config using default Azure credentials
	kustoConfig := &config.KustoConfig{
		Endpoint:            env.Cluster,
		Database:            env.Database,
		TenantID:            env.TenantID,
		UseWorkloadIdentity: true,              // Use default Azure credentials
		TraceTableName:      env.TempTableName, // Use temp table
	}

	// Create plugin config
	pluginConfig := &config.PluginConfig{
		LogLevel: "debug",
	}

	// Create store with temp table
	tempKustoStore, err := store.NewStore(pluginConfig, kustoConfig, env.Logger)
	require.NoError(t, err, "Failed to create temp kusto store")

	env.KustoStore = tempKustoStore
}

// SetupCompleteEnvironment sets up the complete test environment with table, data, and store
func (env *TestEnvironment) SetupCompleteEnvironment(t *testing.T) {
	env.CreateTempTable(t)
	env.IngestTestData(t)
	env.CreateKustoStore(t)
}

// Cleanup cleans up the test environment
func (env *TestEnvironment) Cleanup() {
	// Clean up the table after test
	dropTableCmd := fmt.Sprintf(".drop table %s", env.TempTableName)
	env.Logger.Info("Dropping temporary table", "tableName", env.TempTableName)
	// Use the admin client to drop the temporary table
	// This is done in a deferred function to ensure it runs after the test completes
	_, _ = env.AdminClient.Mgmt(env.Context, env.Database, kql.New("").AddUnsafe(dropTableCmd))

	// Cancel the context
	env.Cancel()
}

// Now your tests can use the common setup
func TestGetServices_Integration(t *testing.T) {
	// Setup common test environment
	env := setupTestEnvironment(t)
	env.SetupCompleteEnvironment(t)

	// c) Run the GetServices service with the filters. Add multiple conditions for predicates
	t.Run("GetServices_AllServices", func(t *testing.T) {
		services, err := env.KustoStore.SpanReader().GetServices(env.Context)
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
}

func TestGetOperations_Integration(t *testing.T) {
	// Setup common test environment
	env := setupTestEnvironment(t)
	env.SetupCompleteEnvironment(t)

	// Test GetOperations with various filter conditions
	t.Run("GetOperations_AllOperations", func(t *testing.T) {
		// Test with no service name and no span kind filters
		query := spanstore.OperationQueryParameters{
			ServiceName: "",
			SpanKind:    "",
		}

		operations, err := env.KustoStore.SpanReader().GetOperations(env.Context, query)
		require.NoError(t, err, "Failed to get operations")

		// Expected operations from our test data
		expectedOperations := map[string]string{
			"http-get-request":  "CLIENT",
			"database-query":    "SERVER",
			"cache-lookup":      "CLIENT",
			"notification-send": "PRODUCER",
			"invalid-span":      "INTERNAL",
		}

		assert.Equal(t, len(expectedOperations), len(operations), "Number of operations should match")

		// Convert to map for easier assertion
		actualOperations := make(map[string]string)
		for _, op := range operations {
			actualOperations[op.Name] = op.SpanKind
		}

		for expectedName, expectedSpanKind := range expectedOperations {
			assert.Contains(t, actualOperations, expectedName, "Operation should be present in results")
			assert.Equal(t, expectedSpanKind, actualOperations[expectedName], "SpanKind should match")
		}
	})

	// Matrix-based tests for service name filtering
	t.Run("GetOperations_ByServiceName_Matrix", func(t *testing.T) {
		// Test cases matrix: serviceName -> expected operations
		testCases := []struct {
			name               string
			serviceName        string
			expectedOperations []spanstore.Operation
			shouldHaveResults  bool
		}{
			{
				name:        "FrontendService",
				serviceName: "frontend-service",
				expectedOperations: []spanstore.Operation{
					{Name: "http-get-request", SpanKind: "CLIENT"},
				},
				shouldHaveResults: true,
			},
			{
				name:        "BackendService",
				serviceName: "backend-service",
				expectedOperations: []spanstore.Operation{
					{Name: "database-query", SpanKind: "SERVER"},
				},
				shouldHaveResults: true,
			},
			{
				name:        "CacheService",
				serviceName: "cache-service",
				expectedOperations: []spanstore.Operation{
					{Name: "cache-lookup", SpanKind: "CLIENT"},
				},
				shouldHaveResults: true,
			},
			{
				name:        "NotificationService",
				serviceName: "notification-service",
				expectedOperations: []spanstore.Operation{
					{Name: "notification-send", SpanKind: "PRODUCER"},
				},
				shouldHaveResults: true,
			},
			{
				name:               "NonExistentService",
				serviceName:        "non-existent-service",
				expectedOperations: []spanstore.Operation{},
				shouldHaveResults:  false,
			},
			{
				name:        "EmptyServiceName",
				serviceName: "",
				expectedOperations: []spanstore.Operation{
					{Name: "http-get-request", SpanKind: "CLIENT"},
					{Name: "database-query", SpanKind: "SERVER"},
					{Name: "cache-lookup", SpanKind: "CLIENT"},
					{Name: "notification-send", SpanKind: "PRODUCER"},
					{Name: "invalid-span", SpanKind: "INTERNAL"},
				},
				shouldHaveResults: true,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				query := spanstore.OperationQueryParameters{
					ServiceName: tc.serviceName,
					SpanKind:    "",
				}

				operations, err := env.KustoStore.SpanReader().GetOperations(env.Context, query)
				require.NoError(t, err, "Failed to get operations for %s", tc.serviceName)

				if !tc.shouldHaveResults {
					assert.Empty(t, operations, "Should return no operations for %s", tc.serviceName)
					return
				}

				assert.Equal(t, len(tc.expectedOperations), len(operations),
					"Number of operations should match for %s", tc.serviceName)

				// Convert to maps for easier comparison
				expectedOpsMap := make(map[string]string)
				for _, op := range tc.expectedOperations {
					expectedOpsMap[op.Name] = op.SpanKind
				}

				actualOpsMap := make(map[string]string)
				for _, op := range operations {
					actualOpsMap[op.Name] = op.SpanKind
				}

				for expectedName, expectedSpanKind := range expectedOpsMap {
					assert.Contains(t, actualOpsMap, expectedName,
						"Operation %s should be present for service %s", expectedName, tc.serviceName)
					assert.Equal(t, expectedSpanKind, actualOpsMap[expectedName],
						"SpanKind should match for operation %s in service %s", expectedName, tc.serviceName)
				}
			})
		}
	})

	// Edge cases and validation tests
	t.Run("GetOperations_EdgeCases", func(t *testing.T) {
		edgeCases := []struct {
			name        string
			serviceName string
			spanKind    string
			description string
		}{
			{
				name:        "EmptyServiceName_EmptySpanKind",
				serviceName: "",
				spanKind:    "",
				description: "Should return all operations when both filters are empty",
			},
			{
				name:        "WhitespaceServiceName",
				serviceName: "   ",
				spanKind:    "",
				description: "Should handle whitespace service names",
			},
			{
				name:        "CaseSensitiveServiceName",
				serviceName: "Frontend-Service", // Different case
				spanKind:    "",
				description: "Should handle case-sensitive service names",
			},
		}

		for _, tc := range edgeCases {
			t.Run(tc.name, func(t *testing.T) {
				query := spanstore.OperationQueryParameters{
					ServiceName: tc.serviceName,
					SpanKind:    tc.spanKind,
				}

				operations, err := env.KustoStore.SpanReader().GetOperations(env.Context, query)
				require.NoError(t, err, "Should not error for edge case: %s", tc.description)

				// Log results for analysis
				t.Logf("Edge case '%s' returned %d operations", tc.name, len(operations))
				for _, op := range operations {
					t.Logf("  - Operation: %s, SpanKind: %s", op.Name, op.SpanKind)
				}
			})
		}
	})
}
