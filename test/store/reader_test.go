//go:build integration
// +build integration

package test

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/dodopizza/jaeger-kusto/config"
	"github.com/dodopizza/jaeger-kusto/store"
	"github.com/hashicorp/go-hclog"

	storagev2 "github.com/dodopizza/jaeger-kusto/proto-gen/storage/v2"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestKustoV2_GetServices(t *testing.T) {
	kustoConfig, _ := config.ParseKustoConfig(testPluginConfig.KustoConfigPath, testPluginConfig.ReadNoTruncation, testPluginConfig.ReadNoTimeout)
	var buf bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{
		Output: &buf,
		Level:  hclog.Debug,
	})
	v2Store, err := store.NewV2Store(kustoConfig, nil, logger)
	if err != nil {
		t.Skipf("Skipping: %v", err)
		return
	}
	defer func() {
		log.SetOutput(os.Stderr)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := v2Store.TraceReader.GetServices(ctx, &storagev2.GetServicesRequest{})
	if err != nil {
		logger.Error("can't get services", "error", err.Error())
	}
	fmt.Printf("Services: %+v\n", resp.GetServices())
	assert.NotNil(t, resp)
}

func TestKustoV2_GetOperations(t *testing.T) {
	kustoConfig, _ := config.ParseKustoConfig(testPluginConfig.KustoConfigPath, testPluginConfig.ReadNoTruncation, testPluginConfig.ReadNoTimeout)
	v2Store, err := store.NewV2Store(kustoConfig, nil, logger)
	if err != nil {
		t.Skipf("Skipping: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := v2Store.TraceReader.GetOperations(ctx, &storagev2.GetOperationsRequest{
		Service: "frontend",
	})
	if err != nil {
		logger.Error("can't get operations", "error", err.Error())
	}
	fmt.Printf("Operations: %+v\n", resp.GetOperations())
}

func TestKustoV2_FindTraceIDs(t *testing.T) {
	kustoConfig, _ := config.ParseKustoConfig(testPluginConfig.KustoConfigPath, testPluginConfig.ReadNoTruncation, testPluginConfig.ReadNoTimeout)
	v2Store, err := store.NewV2Store(kustoConfig, nil, logger)
	if err != nil {
		t.Skipf("Skipping: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := v2Store.TraceReader.FindTraceIDs(ctx, &storagev2.FindTracesRequest{
		Query: &storagev2.TraceQueryParameters{
			ServiceName:  "my-service",
			StartTimeMin: timestamppb.New(time.Date(2023, time.January, 29, 6, 0, 0, 0, time.UTC)),
			StartTimeMax: timestamppb.New(time.Date(2023, time.January, 30, 23, 0, 0, 0, time.UTC)),
			SearchDepth:  20,
		},
	})
	if err != nil {
		logger.Error("can't find trace IDs", "error", err.Error())
	}
	fmt.Printf("TraceIDs: %d found\n", len(resp.GetTraceIds()))
}

func TestKustoV2_GetDependencies(t *testing.T) {
	kustoConfig, _ := config.ParseKustoConfig(testPluginConfig.KustoConfigPath, testPluginConfig.ReadNoTruncation, testPluginConfig.ReadNoTimeout)
	v2Store, err := store.NewV2Store(kustoConfig, nil, logger)
	if err != nil {
		t.Skipf("Skipping: %v", err)
		return
	}

	now := time.Now()
	resp, err := v2Store.DependencyReader.GetDependencies(context.Background(), &storagev2.GetDependenciesRequest{
		StartTime: timestamppb.New(now.Add(-2 * time.Hour)),
		EndTime:   timestamppb.New(now),
	})
	if err != nil {
		logger.Error("can't get dependencies", "error", err.Error())
	}
	fmt.Printf("Dependencies: %+v\n", resp.GetDependencies())
}

