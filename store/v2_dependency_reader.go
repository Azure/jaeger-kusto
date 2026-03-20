package store

import (
	"context"
	"fmt"

	"github.com/hashicorp/go-hclog"

	storagev2 "github.com/dodopizza/jaeger-kusto/proto-gen/storage/v2"
)

// DependencyReaderService implements the V2 DependencyReader gRPC service.
type DependencyReaderService struct {
	storagev2.UnimplementedDependencyReaderServer
	reader *kustoV2Reader
	logger hclog.Logger
}

// NewDependencyReaderService creates a new DependencyReaderService.
func NewDependencyReaderService(reader *kustoV2Reader, logger hclog.Logger) *DependencyReaderService {
	return &DependencyReaderService{
		reader: reader,
		logger: logger,
	}
}

// GetDependencies loads service dependencies from storage.
func (s *DependencyReaderService) GetDependencies(ctx context.Context, req *storagev2.GetDependenciesRequest) (*storagev2.GetDependenciesResponse, error) {
	startTime := req.GetStartTime().AsTime()
	endTime := req.GetEndTime().AsTime()

	// Try cache first
	if deps, ok := s.reader.getCachedDependencies(ctx, startTime, endTime); ok {
		return depsToProto(deps), nil
	}

	deps, err := s.reader.fetchDependencies(ctx, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("failed to get dependencies: %w", err)
	}

	return depsToProto(deps), nil
}

func depsToProto(deps []dependencyResult) *storagev2.GetDependenciesResponse {
	result := make([]*storagev2.Dependency, 0, len(deps))
	for _, d := range deps {
		result = append(result, &storagev2.Dependency{
			Parent:    d.Parent,
			Child:     d.Child,
			CallCount: uint64(d.CallCount),
		})
	}
	return &storagev2.GetDependenciesResponse{
		Dependencies: result,
	}
}
