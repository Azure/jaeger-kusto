package store

import (
	"context"
	"fmt"
	"time"

	"github.com/hashicorp/go-hclog"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"

	storagev2 "github.com/dodopizza/jaeger-kusto/proto-gen/storage/v2"
)

// TraceReaderService implements the V2 TraceReader gRPC service.
type TraceReaderService struct {
	storagev2.UnimplementedTraceReaderServer
	reader *kustoV2Reader
	logger hclog.Logger
}

// NewTraceReaderService creates a new TraceReaderService.
func NewTraceReaderService(reader *kustoV2Reader, logger hclog.Logger) *TraceReaderService {
	return &TraceReaderService{
		reader: reader,
		logger: logger,
	}
}

// GetTraces returns a stream of TracesData for the requested trace IDs.
func (s *TraceReaderService) GetTraces(req *storagev2.GetTracesRequest, stream grpc.ServerStreamingServer[tracepb.TracesData]) error {
	ctx := stream.Context()

	for _, param := range req.GetQuery() {
		traceID := fmt.Sprintf("%x", param.GetTraceId())
		if traceID == "" {
			continue
		}

		spans, err := s.reader.getTraceByID(ctx, traceID)
		if err != nil {
			s.logger.Error("GetTraces: error fetching trace", "traceID", traceID, "error", err)
			return fmt.Errorf("failed to get trace %s: %w", traceID, err)
		}

		if len(spans) == 0 {
			continue
		}

		tracesDataList := groupSpansIntoTracesData(spans, s.logger)
		for _, td := range tracesDataList {
			if err := stream.Send(td); err != nil {
				return fmt.Errorf("failed to send trace data: %w", err)
			}
		}
	}

	return nil
}

// GetServices returns all service names known to the backend.
func (s *TraceReaderService) GetServices(ctx context.Context, _ *storagev2.GetServicesRequest) (*storagev2.GetServicesResponse, error) {
	services, err := s.reader.getServices(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get services: %w", err)
	}

	return &storagev2.GetServicesResponse{
		Services: services,
	}, nil
}

// GetOperations returns all operations for a given service.
func (s *TraceReaderService) GetOperations(ctx context.Context, req *storagev2.GetOperationsRequest) (*storagev2.GetOperationsResponse, error) {
	ops, err := s.reader.getOperations(ctx, req.GetService(), req.GetSpanKind())
	if err != nil {
		return nil, fmt.Errorf("failed to get operations: %w", err)
	}

	result := make([]*storagev2.Operation, 0, len(ops))
	for _, op := range ops {
		result = append(result, &storagev2.Operation{
			Name:     op.OperationName,
			SpanKind: spanKindToJaegerString(op.SpanKind),
		})
	}

	return &storagev2.GetOperationsResponse{
		Operations: result,
	}, nil
}

// FindTraces returns a stream of TracesData matching the query parameters.
func (s *TraceReaderService) FindTraces(req *storagev2.FindTracesRequest, stream grpc.ServerStreamingServer[tracepb.TracesData]) error {
	ctx := stream.Context()
	q := req.GetQuery()
	if q == nil {
		return nil
	}

	params := protoQueryToParams(q)
	spans, err := s.reader.findTraces(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to find traces: %w", err)
	}

	if len(spans) == 0 {
		return nil
	}

	tracesDataList := groupSpansIntoTracesData(spans, s.logger)
	for _, td := range tracesDataList {
		if err := stream.Send(td); err != nil {
			return fmt.Errorf("failed to send trace data: %w", err)
		}
	}

	return nil
}

// FindTraceIDs returns trace IDs matching the query parameters.
func (s *TraceReaderService) FindTraceIDs(ctx context.Context, req *storagev2.FindTracesRequest) (*storagev2.FindTraceIDsResponse, error) {
	q := req.GetQuery()
	if q == nil {
		return &storagev2.FindTraceIDsResponse{}, nil
	}

	params := protoQueryToParams(q)
	ids, err := s.reader.findTraceIDs(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to find trace IDs: %w", err)
	}

	result := make([]*storagev2.FoundTraceID, 0, len(ids))
	for _, id := range ids {
		result = append(result, &storagev2.FoundTraceID{
			TraceId: hexToBytes(id),
		})
	}

	return &storagev2.FindTraceIDsResponse{
		TraceIds: result,
	}, nil
}

// protoQueryToParams converts a V2 proto TraceQueryParameters to our internal params.
func protoQueryToParams(q *storagev2.TraceQueryParameters) *traceQueryParams {
	params := &traceQueryParams{
		ServiceName:   q.GetServiceName(),
		OperationName: q.GetOperationName(),
		SearchDepth:   q.GetSearchDepth(),
	}

	if q.GetStartTimeMin() != nil {
		params.StartTimeMin = q.GetStartTimeMin().AsTime()
	}
	if q.GetStartTimeMax() != nil {
		params.StartTimeMax = q.GetStartTimeMax().AsTime()
	}
	if q.GetDurationMin() != nil {
		params.DurationMin = q.GetDurationMin().AsDuration()
	}
	if q.GetDurationMax() != nil {
		params.DurationMax = q.GetDurationMax().AsDuration()
	}

	// Convert proto attributes to string map
	if len(q.GetAttributes()) > 0 {
		params.Attributes = make(map[string]string, len(q.GetAttributes()))
		for _, kv := range q.GetAttributes() {
			key := kv.GetKey()
			val := extractStringValue(kv.GetValue())
			if key != "" && val != "" {
				params.Attributes[key] = val
			}
		}
	}

	// Ensure time range is set
	if params.StartTimeMin.IsZero() {
		params.StartTimeMin = time.Now().Add(-24 * time.Hour)
	}
	if params.StartTimeMax.IsZero() {
		params.StartTimeMax = time.Now()
	}

	return params
}

// extractStringValue extracts a string from a V2 proto AnyValue.
func extractStringValue(v *storagev2.AnyValue) string {
	if v == nil {
		return ""
	}
	switch val := v.GetValue().(type) {
	case *storagev2.AnyValue_StringValue:
		return val.StringValue
	case *storagev2.AnyValue_BoolValue:
		return fmt.Sprintf("%v", val.BoolValue)
	case *storagev2.AnyValue_IntValue:
		return fmt.Sprintf("%d", val.IntValue)
	case *storagev2.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", val.DoubleValue)
	default:
		return ""
	}
}
