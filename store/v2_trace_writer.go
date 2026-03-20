package store

import (
	"context"

	"github.com/hashicorp/go-hclog"

	collectortrace "github.com/dodopizza/jaeger-kusto/proto-gen/collector/trace/v1"
)

// TraceWriterService implements the OTEL TraceService as a no-op.
// Span ingestion into Kusto is handled externally by the OTEL Collector's ADX exporter.
type TraceWriterService struct {
	collectortrace.UnimplementedTraceServiceServer
	logger hclog.Logger
}

// NewTraceWriterService creates a new no-op TraceWriterService.
func NewTraceWriterService(logger hclog.Logger) *TraceWriterService {
	return &TraceWriterService{logger: logger}
}

// Export accepts trace data but does not persist it. Returns success.
func (s *TraceWriterService) Export(_ context.Context, _ *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	s.logger.Debug("TraceWriterService.Export called (no-op)")
	return &collectortrace.ExportTraceServiceResponse{}, nil
}
