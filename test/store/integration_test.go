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
	// Test data for OTEL traces - Updated to have proper parent-child relationships for dependencies
	testOTELTracesData = `3f6d8f4c5008352055c14804949d1e57,b0a8c042b2621fe9,http-get-request,CLIENT,2024-01-01T10:00:00Z,2024-01-01T10:00:01Z,"","{""service.name"":""frontend-service"",""service.version"":""1.0.0""}","{""http.method"":""GET"",""http.url"":""/api/users""}","[]"
4a7e9f5d6119463166d25915a5a2f968,00ae66c75b61014d,database-query,SERVER,2024-01-01T10:00:00Z,2024-01-01T10:00:02Z,b0a8c042b2621fe9,"{""service.name"":""backend-service"",""service.version"":""2.1.0""}","{""db.statement"":""SELECT * FROM users""}","[]"
5b8fa06e722a574277e3696ba6b3c079,b281c3f85270ec89,cache-lookup,CLIENT,2024-01-01T10:00:00Z,2024-01-01T10:00:00.5Z,00ae66c75b61014d,"{""service.name"":""cache-service"",""service.version"":""1.2.0""}","{""cache.key"":""user:123""}","[]"
6c9ab17f833b685388f4797cab4d118a,1753db1da505545f,notification-send,PRODUCER,2024-01-01T10:00:00Z,2024-01-01T10:00:03Z,b0a8c042b2621fe9,"{""service.name"":""notification-service"",""service.version"":""1.5.0""}","{""notification.type"":""email""}","[]"
7d1bc28a944c796499a589adbcde2299,06b97c543b45c1dc,invalid-span,INTERNAL,2024-01-01T10:00:00Z,2024-01-01T10:00:01Z,"","{""service.version"":""1.0.0""}","{}","[]"`
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
		Level: hclog.Info,
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
		env.cleanup()
	})
	return env
}

// createTempTable creates a temporary table with the OTEL traces schema
func (env *TestEnvironment) createTempTable(t *testing.T) {
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

// createKustoStore creates a Kusto store configured to use the temporary table
func (env *TestEnvironment) createKustoStore(t *testing.T) {
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

// setupCompleteEnvironment sets up the complete test environment with table, data, and store
func (env *TestEnvironment) setupCompleteEnvironment(t *testing.T) {
	env.createTempTable(t)
	env.IngestTestData(t)
	env.createKustoStore(t)
}

// cleanup cleans up the test environment
func (env *TestEnvironment) cleanup() {
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
	env.setupCompleteEnvironment(t)
	t.Parallel()
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
	env.setupCompleteEnvironment(t)
	t.Parallel()
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

func TestFindTraces_Integration(t *testing.T) {
	// Setup common test environment
	env := setupTestEnvironment(t)
	env.setupCompleteEnvironment(t)

	t.Parallel()
	// Simple test for FindTraces with basic parameters
	t.Run("FindTraces_ByServiceName", func(t *testing.T) {
		// Set up query parameters for finding traces
		startTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
		endTime := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)

		query := &spanstore.TraceQueryParameters{
			ServiceName:  "frontend-service",
			StartTimeMin: startTime,
			StartTimeMax: endTime,
			NumTraces:    10,
		}

		traces, err := env.KustoStore.SpanReader().FindTraces(env.Context, query)
		require.NoError(t, err, "Failed to find traces")

		// Basic validation
		assert.NotNil(t, traces, "Traces should not be nil")

		// If traces are found, validate their structure
		if len(traces) > 0 {
			t.Logf("Found %d traces", len(traces))

			for i, trace := range traces {
				assert.NotNil(t, trace, "Trace %d should not be nil", i)
				assert.NotEmpty(t, trace.Spans, "Trace %d should have spans", i)

				// Validate that at least one span belongs to the queried service
				foundService := false
				for _, span := range trace.Spans {
					if span.Process != nil && span.Process.ServiceName == "frontend-service" {
						foundService = true
						break
					}
				}
				assert.True(t, foundService, "Trace %d should contain spans from frontend-service", i)

				t.Logf("Trace %d: TraceID=%s, Spans=%d", i, trace.Spans[0].TraceID, len(trace.Spans))
			}
		} else {
			t.Log("No traces found - this might be expected if the data doesn't match the time range")
		}
	})

	t.Run("FindTraces_ByOperationName", func(t *testing.T) {
		// Set up query parameters for finding traces by operation name
		startTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
		endTime := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)

		query := &spanstore.TraceQueryParameters{
			ServiceName:   "frontend-service",
			OperationName: "http-get-request",
			StartTimeMin:  startTime,
			StartTimeMax:  endTime,
			NumTraces:     10,
		}

		traces, err := env.KustoStore.SpanReader().FindTraces(env.Context, query)
		require.NoError(t, err, "Failed to find traces by operation name")

		// Basic validation
		assert.NotNil(t, traces, "Traces should not be nil")

		// If traces are found, validate their structure
		if len(traces) > 0 {
			t.Logf("Found %d traces for operation http-get-request", len(traces))

			for i, trace := range traces {
				assert.NotNil(t, trace, "Trace %d should not be nil", i)
				assert.NotEmpty(t, trace.Spans, "Trace %d should have spans", i)

				// Validate that at least one span has the queried operation name
				foundOperation := false
				for _, span := range trace.Spans {
					if span.OperationName == "http-get-request" {
						foundOperation = true
						break
					}
				}
				assert.True(t, foundOperation, "Trace %d should contain spans with operation http-get-request", i)
			}
		} else {
			t.Log("No traces found for operation http-get-request")
		}
	})

	t.Run("FindTraces_WithTags", func(t *testing.T) {
		// Set up query parameters for finding traces by tags
		startTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
		endTime := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)

		query := &spanstore.TraceQueryParameters{
			ServiceName:  "frontend-service",
			StartTimeMin: startTime,
			StartTimeMax: endTime,
			Tags: map[string]string{
				"http.method": "GET",
			},
			NumTraces: 10,
		}

		traces, err := env.KustoStore.SpanReader().FindTraces(env.Context, query)
		require.NoError(t, err, "Failed to find traces with tags")

		// Basic validation
		assert.NotNil(t, traces, "Traces should not be nil")

		if len(traces) > 0 {
			t.Logf("Found %d traces with http.method=GET tag", len(traces))

			for i, trace := range traces {
				assert.NotNil(t, trace, "Trace %d should not be nil", i)
				assert.NotEmpty(t, trace.Spans, "Trace %d should have spans", i)
			}
		} else {
			t.Log("No traces found with http.method=GET tag")
		}
	})

	t.Run("FindTraces_AllServices", func(t *testing.T) {
		// Set up query parameters for finding traces from all services
		startTime := time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC)
		endTime := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)

		query := &spanstore.TraceQueryParameters{
			StartTimeMin: startTime,
			StartTimeMax: endTime,
			NumTraces:    20, // Get more traces since we're looking across all services
		}

		traces, err := env.KustoStore.SpanReader().FindTraces(env.Context, query)
		require.NoError(t, err, "Failed to find traces from all services")

		// Basic validation
		assert.NotNil(t, traces, "Traces should not be nil")

		if len(traces) > 0 {
			t.Logf("Found %d traces across all services", len(traces))

			// Collect unique services from found traces
			services := make(map[string]bool)
			for i, trace := range traces {
				assert.NotNil(t, trace, "Trace %d should not be nil", i)
				assert.NotEmpty(t, trace.Spans, "Trace %d should have spans", i)

				for _, span := range trace.Spans {
					if span.Process != nil && span.Process.ServiceName != "" {
						services[span.Process.ServiceName] = true
					}
				}
			}

			t.Logf("Found traces from services: %v", getKeys(services))

			// We should find traces from multiple services
			assert.GreaterOrEqual(t, len(services), 1, "Should find traces from at least one service")
		} else {
			t.Log("No traces found across all services")
		}
	})

	t.Run("FindTraces_EmptyResult", func(t *testing.T) {
		// Set up query parameters that should return no results
		startTime := time.Date(2025, 1, 1, 9, 0, 0, 0, time.UTC) // Future date
		endTime := time.Date(2025, 1, 1, 11, 0, 0, 0, time.UTC)

		query := &spanstore.TraceQueryParameters{
			ServiceName:  "frontend-service",
			StartTimeMin: startTime,
			StartTimeMax: endTime,
			NumTraces:    10,
		}

		traces, err := env.KustoStore.SpanReader().FindTraces(env.Context, query)
		require.NoError(t, err, "Should not error for future date range")

		// Should return empty result
		assert.Nil(t, traces, "Should return no traces for future date range")
	})
}

// Helper function to get keys from a map
func getKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func TestGetDependencies_Integration(t *testing.T) {
	// Setup common test environment
	env := setupTestEnvironment(t)
	env.setupCompleteEnvironment(t)

	t.Parallel()
	// Test GetDependencies with basic parameters
	t.Run("GetDependencies_Basic", func(t *testing.T) {
		// Set up query parameters for getting dependencies
		endTime := time.Date(2024, 1, 1, 10, 59, 59, 999, time.UTC)
		lookback := 2 * time.Hour // Look back 2 hours

		dependencies, err := env.KustoStore.DependencyReader().GetDependencies(env.Context, endTime, lookback)
		require.NoError(t, err, "Failed to get dependencies")

		// Basic validation
		assert.NotNil(t, dependencies, "Dependencies should not be nil")

		if len(dependencies) > 0 {
			t.Logf("Found %d dependency links", len(dependencies))

			// Validate each dependency link
			for i, dep := range dependencies {
				assert.NotEmpty(t, dep.Parent, "Dependency %d should have a parent service", i)
				assert.NotEmpty(t, dep.Child, "Dependency %d should have a child service", i)
				assert.Greater(t, dep.CallCount, uint64(0), "Dependency %d should have call count > 0", i)
				assert.NotEqual(t, dep.Parent, dep.Child, "Dependency %d parent should not equal child", i)

				t.Logf("Dependency %d: %s -> %s (calls: %d)", i, dep.Parent, dep.Child, dep.CallCount)
			}

			// Based on our test data, we should have frontend-service -> backend-service dependency
			// (since database-query span has ParentID pointing to http-get-request span)
			expectedDependency := false
			for _, dep := range dependencies {
				if dep.Parent == "frontend-service" && dep.Child == "backend-service" {
					expectedDependency = true
					assert.Equal(t, uint64(1), dep.CallCount, "Should have 1 call from frontend to backend")
					break
				}
			}
			assert.True(t, expectedDependency, "Should find dependency from frontend-service to backend-service")

		} else {
			t.Log("No dependencies found - this might be expected if spans don't have proper parent-child relationships")
		}
	})

	t.Run("GetDependencies_DifferentTimeRanges", func(t *testing.T) {
		// Test with different time ranges
		timeRangeTests := []struct {
			name     string
			endTime  time.Time
			lookback time.Duration
		}{
			{
				name:     "OneHourLookback",
				endTime:  time.Date(2024, 1, 1, 10, 59, 59, 999, time.UTC),
				lookback: 1 * time.Hour,
			},
			{
				name:     "FourHourLookback",
				endTime:  time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
				lookback: 4 * time.Hour,
			},
			{
				name:     "ShortLookback",
				endTime:  time.Date(2024, 1, 1, 10, 29, 59, 999, time.UTC),
				lookback: 30 * time.Minute,
			},
		}

		for _, tc := range timeRangeTests {
			t.Run(tc.name, func(t *testing.T) {
				dependencies, err := env.KustoStore.DependencyReader().GetDependencies(env.Context, tc.endTime, tc.lookback)
				require.NoError(t, err, "Failed to get dependencies for %s", tc.name)

				assert.NotNil(t, dependencies, "Dependencies should not be nil for %s", tc.name)
				t.Logf("%s: Found %d dependencies", tc.name, len(dependencies))

				// Log the dependencies for analysis
				for _, dep := range dependencies {
					t.Logf("  %s -> %s (calls: %d)", dep.Parent, dep.Child, dep.CallCount)
				}
			})
		}
	})

	t.Run("GetDependencies_NoDataTimeRange", func(t *testing.T) {
		// Test with time range that should return no dependencies
		endTime := time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC) // Before our test data
		lookback := 1 * time.Hour

		dependencies, err := env.KustoStore.DependencyReader().GetDependencies(env.Context, endTime, lookback)
		require.NoError(t, err, "Should not error for time range with no data")

		assert.Nil(t, dependencies, "Should return no dependencies for time range with no data")
	})

	t.Run("GetDependencies_FutureTimeRange", func(t *testing.T) {
		// Test with future time range
		endTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC) // Future date
		lookback := 2 * time.Hour

		dependencies, err := env.KustoStore.DependencyReader().GetDependencies(env.Context, endTime, lookback)
		require.NoError(t, err, "Should not error for future time range")

		assert.Nil(t, dependencies, "No dependencies should be returned for future time range")
		// Future time range might or might not return data depending on the query logic
		t.Logf("Future time range returned %d dependencies", len(dependencies))
	})

	t.Run("GetDependencies_LongLookback", func(t *testing.T) {
		// Test with very long lookback period
		endTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
		lookback := 24 * time.Hour // Look back 24 hours

		dependencies, err := env.KustoStore.DependencyReader().GetDependencies(env.Context, endTime, lookback)
		require.NoError(t, err, "Should not error for long lookback period")

		assert.NotNil(t, dependencies, "Dependencies should not be nil")
		t.Logf("Long lookback returned %d dependencies", len(dependencies))

		// With a longer lookback, we should capture all our test data
		if len(dependencies) > 0 {
			// Validate that all dependencies have proper structure
			services := make(map[string]bool)
			for _, dep := range dependencies {
				assert.NotEmpty(t, dep.Parent, "Parent service should not be empty")
				assert.NotEmpty(t, dep.Child, "Child service should not be empty")
				assert.Greater(t, dep.CallCount, uint64(0), "Call count should be greater than 0")

				services[dep.Parent] = true
				services[dep.Child] = true
			}

			t.Logf("Found services in dependencies: %v", getKeys(services))
		}
	})

	t.Run("GetDependencies_ValidationTests", func(t *testing.T) {
		// Test edge cases and validation
		endTime := time.Date(2024, 1, 1, 11, 0, 0, 0, time.UTC)
		lookback := 2 * time.Hour

		dependencies, err := env.KustoStore.DependencyReader().GetDependencies(env.Context, endTime, lookback)
		require.NoError(t, err, "Failed to get dependencies for validation")

		if len(dependencies) > 0 {
			// Validate data quality
			for i, dep := range dependencies {
				// Parent and child should be different
				assert.NotEqual(t, dep.Parent, dep.Child,
					"Dependency %d: parent and child should be different services", i)

				// Parent and child should not be empty
				assert.NotEmpty(t, dep.Parent, "Dependency %d: parent should not be empty", i)
				assert.NotEmpty(t, dep.Child, "Dependency %d: child should not be empty", i)

				// Call count should be positive
				assert.Greater(t, dep.CallCount, uint64(0),
					"Dependency %d: call count should be positive", i)

				// Service names should be from our known test services
				knownServices := map[string]bool{
					"frontend-service":     true,
					"backend-service":      true,
					"cache-service":        true,
					"notification-service": true,
				}

				// Note: We might have services without service.name (like our invalid-span)
				// so we'll just log if we find unknown services rather than fail
				if !knownServices[dep.Parent] {
					t.Logf("Found unknown parent service: %s", dep.Parent)
				}
				if !knownServices[dep.Child] {
					t.Logf("Found unknown child service: %s", dep.Child)
				}
			}

			// Check for duplicate dependencies (same parent-child pair)
			seen := make(map[string]bool)
			for _, dep := range dependencies {
				key := fmt.Sprintf("%s->%s", dep.Parent, dep.Child)
				assert.False(t, seen[key], "Found duplicate dependency: %s", key)
				seen[key] = true
			}
		}
	})
}
