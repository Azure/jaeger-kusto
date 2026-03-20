package store

import (
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func TestConvertOTLPSpanToProto(t *testing.T) {
	span := &otlpSpan{
		TraceID:            "d1b06c73d963045e657158dbd0ccf6d9",
		SpanID:             "cfb683d327e4dd90",
		ParentID:           "1234567890abcdef",
		SpanName:           "GET /api/users",
		SpanKind:           "SPAN_KIND_SERVER",
		SpanStatus:         "STATUS_CODE_OK",
		SpanStatusMessage:  "",
		StartTime:          time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		EndTime:            time.Date(2024, 1, 1, 0, 0, 0, 100000000, time.UTC),
		ResourceAttributes: `{"service.name":"my-service","host.name":"host-1"}`,
		TraceAttributes:    `{"http.method":"GET","http.url":"/api/users","http.status_code":200}`,
		Events:             `[]`,
		Links:              `[]`,
	}

	result := convertOTLPSpanToProto(span)

	assert.NotNil(t, result)
	assert.Equal(t, "GET /api/users", result.Name)
	assert.Equal(t, tracepb.Span_SPAN_KIND_SERVER, result.Kind)
	assert.Equal(t, tracepb.Status_STATUS_CODE_OK, result.Status.Code)
	assert.Len(t, result.TraceId, 16)
	assert.Len(t, result.SpanId, 8)
	assert.Len(t, result.ParentSpanId, 8)
	assert.NotZero(t, result.StartTimeUnixNano)
	assert.NotZero(t, result.EndTimeUnixNano)
	assert.True(t, result.EndTimeUnixNano > result.StartTimeUnixNano)
}

func TestConvertOTLPSpanToProto_ErrorStatus(t *testing.T) {
	span := &otlpSpan{
		TraceID:    "d1b06c73d963045e657158dbd0ccf6d9",
		SpanID:     "cfb683d327e4dd90",
		SpanName:   "POST /api/error",
		SpanKind:   "SPAN_KIND_CLIENT",
		SpanStatus: "STATUS_CODE_ERROR",
		SpanStatusMessage: "connection refused",
		StartTime:  time.Now(),
		EndTime:    time.Now(),
	}

	result := convertOTLPSpanToProto(span)
	assert.Equal(t, tracepb.Span_SPAN_KIND_CLIENT, result.Kind)
	assert.Equal(t, tracepb.Status_STATUS_CODE_ERROR, result.Status.Code)
	assert.Equal(t, "connection refused", result.Status.Message)
}

func TestGroupSpansIntoTracesData(t *testing.T) {
	logger := hclog.Default()
	spans := []*otlpSpan{
		{
			TraceID:            "aaaa",
			SpanID:             "1111",
			SpanName:           "span-1",
			StartTime:          time.Now(),
			EndTime:            time.Now(),
			ResourceAttributes: `{"service.name":"svc-a"}`,
		},
		{
			TraceID:            "aaaa",
			SpanID:             "2222",
			SpanName:           "span-2",
			StartTime:          time.Now(),
			EndTime:            time.Now(),
			ResourceAttributes: `{"service.name":"svc-b"}`,
		},
		{
			TraceID:            "bbbb",
			SpanID:             "3333",
			SpanName:           "span-3",
			StartTime:          time.Now(),
			EndTime:            time.Now(),
			ResourceAttributes: `{"service.name":"svc-a"}`,
		},
	}

	result := groupSpansIntoTracesData(spans, logger)
	require.Len(t, result, 2) // two distinct traces

	// First trace should have 2 spans (from aaaa)
	trace1 := result[0]
	totalSpans1 := 0
	for _, rs := range trace1.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			totalSpans1 += len(ss.Spans)
		}
	}
	assert.Equal(t, 2, totalSpans1)

	// Second trace should have 1 span (from bbbb)
	trace2 := result[1]
	totalSpans2 := 0
	for _, rs := range trace2.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			totalSpans2 += len(ss.Spans)
		}
	}
	assert.Equal(t, 1, totalSpans2)
}

func TestParseAttributes(t *testing.T) {
	kvs := parseAttributes(`{"key1":"value1","key2":42,"key3":true}`)
	assert.Len(t, kvs, 3)

	// Empty/null cases
	assert.Nil(t, parseAttributes(""))
	assert.Nil(t, parseAttributes("{}"))
	assert.Nil(t, parseAttributes("null"))
}

func TestParseEvents(t *testing.T) {
	events := parseEvents(`[{"EventName":"exception","Timestamp":"2024-01-01T00:00:00Z","EventAttributes":{"exception.type":"RuntimeError"}}]`)
	require.Len(t, events, 1)
	assert.Equal(t, "exception", events[0].Name)
	assert.NotZero(t, events[0].TimeUnixNano)
	assert.Len(t, events[0].Attributes, 1)

	assert.Nil(t, parseEvents("[]"))
	assert.Nil(t, parseEvents(""))
}

func TestParseLinks(t *testing.T) {
	links := parseLinks(`[{"TraceID":"aabb","SpanID":"ccdd","TraceState":"","SpanLinkAttributes":{}}]`)
	require.Len(t, links, 1)
	assert.Equal(t, hexToBytes("aabb"), links[0].TraceId)
	assert.Equal(t, hexToBytes("ccdd"), links[0].SpanId)

	assert.Nil(t, parseLinks("[]"))
	assert.Nil(t, parseLinks(""))
}

func TestSpanKindFromString(t *testing.T) {
	assert.Equal(t, tracepb.Span_SPAN_KIND_SERVER, spanKindFromString("SPAN_KIND_SERVER"))
	assert.Equal(t, tracepb.Span_SPAN_KIND_CLIENT, spanKindFromString("SPAN_KIND_CLIENT"))
	assert.Equal(t, tracepb.Span_SPAN_KIND_UNSPECIFIED, spanKindFromString("UNKNOWN"))
}

func TestHexToBytes(t *testing.T) {
	assert.Equal(t, []byte{0xde, 0xad}, hexToBytes("dead"))
	assert.Nil(t, hexToBytes(""))
	assert.Nil(t, hexToBytes("zz"))
}

func TestExtractServiceName(t *testing.T) {
	assert.Equal(t, "my-service", extractServiceName(`{"service.name":"my-service"}`))
	assert.Equal(t, "", extractServiceName(`{}`))
	assert.Equal(t, "", extractServiceName(""))
}
