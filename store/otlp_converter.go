package store

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// otlpSpan holds a Kusto row representing an OTEL span, deserialized using
// kusto struct tags. This is used for the V2 (OTLP-native) code path.
type otlpSpan struct {
	TraceID            string    `kusto:"TraceID"`
	SpanID             string    `kusto:"SpanID"`
	ParentID           string    `kusto:"ParentID"`
	SpanName           string    `kusto:"SpanName"`
	SpanKind           string    `kusto:"SpanKind"`
	SpanStatus         string    `kusto:"SpanStatus"`
	SpanStatusMessage  string    `kusto:"SpanStatusMessage"`
	StartTime          time.Time `kusto:"StartTime"`
	EndTime            time.Time `kusto:"EndTime"`
	ResourceAttributes string    `kusto:"ResourceAttributes"`
	TraceAttributes    string    `kusto:"TraceAttributes"`
	Events             string    `kusto:"Events"`
	Links              string    `kusto:"Links"`
}

// spanKindFromString maps the Kusto SpanKind string to the OTLP proto enum.
func spanKindFromString(s string) tracepb.Span_SpanKind {
	switch s {
	case "SPAN_KIND_INTERNAL":
		return tracepb.Span_SPAN_KIND_INTERNAL
	case "SPAN_KIND_SERVER":
		return tracepb.Span_SPAN_KIND_SERVER
	case "SPAN_KIND_CLIENT":
		return tracepb.Span_SPAN_KIND_CLIENT
	case "SPAN_KIND_PRODUCER":
		return tracepb.Span_SPAN_KIND_PRODUCER
	case "SPAN_KIND_CONSUMER":
		return tracepb.Span_SPAN_KIND_CONSUMER
	default:
		return tracepb.Span_SPAN_KIND_UNSPECIFIED
	}
}

// statusCodeFromString maps the Kusto SpanStatus string to the OTLP status code.
func statusCodeFromString(s string) tracepb.Status_StatusCode {
	switch s {
	case "STATUS_CODE_OK":
		return tracepb.Status_STATUS_CODE_OK
	case "STATUS_CODE_ERROR":
		return tracepb.Status_STATUS_CODE_ERROR
	default:
		return tracepb.Status_STATUS_CODE_UNSET
	}
}

// hexToBytes decodes a hex-encoded string to bytes.
// Returns nil on empty string or decode error.
func hexToBytes(s string) []byte {
	if s == "" {
		return nil
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil
	}
	return b
}

// hexToBytesOrNilIfZero decodes a hex string but returns nil if all bytes are zero.
// Used for ParentSpanId where all-zeros means "no parent" (root span).
func hexToBytesOrNilIfZero(s string) []byte {
	b := hexToBytes(s)
	if b == nil {
		return nil
	}
	for _, v := range b {
		if v != 0 {
			return b
		}
	}
	return nil
}

// toUnixNano converts a time.Time to nanoseconds since epoch.
func toUnixNano(t time.Time) uint64 {
	if t.IsZero() {
		return 0
	}
	return uint64(t.UnixNano())
}

// parseAttributes parses a JSON string (Kusto dynamic column) into OTLP KeyValue pairs.
func parseAttributes(raw string) []*commonpb.KeyValue {
	if raw == "" || raw == "{}" || raw == "null" {
		return nil
	}

	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil
	}

	kvs := make([]*commonpb.KeyValue, 0, len(m))
	for k, v := range m {
		kvs = append(kvs, &commonpb.KeyValue{
			Key:   k,
			Value: toAnyValue(v),
		})
	}
	return kvs
}

// toAnyValue converts a Go interface{} value to an OTLP AnyValue.
func toAnyValue(v interface{}) *commonpb.AnyValue {
	if v == nil {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: ""}}
	}
	switch val := v.(type) {
	case string:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: val}}
	case bool:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: val}}
	case float64:
		if val == float64(int64(val)) {
			return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: int64(val)}}
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: val}}
	case []interface{}:
		arr := &commonpb.ArrayValue{Values: make([]*commonpb.AnyValue, 0, len(val))}
		for _, item := range val {
			arr.Values = append(arr.Values, toAnyValue(item))
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: arr}}
	case map[string]interface{}:
		kvl := &commonpb.KeyValueList{Values: make([]*commonpb.KeyValue, 0, len(val))}
		for k, item := range val {
			kvl.Values = append(kvl.Values, &commonpb.KeyValue{
				Key:   k,
				Value: toAnyValue(item),
			})
		}
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: kvl}}
	default:
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: fmt.Sprint(v)}}
	}
}

// otlpEvent is the JSON structure for an event stored in the Kusto Events column.
type otlpEvent struct {
	EventName       string                 `json:"EventName"`
	Timestamp       string                 `json:"Timestamp"`
	EventAttributes map[string]interface{} `json:"EventAttributes"`
}

// parseEvents parses the Kusto Events JSON column into OTLP Span_Events.
func parseEvents(raw string) []*tracepb.Span_Event {
	if raw == "" || raw == "[]" || raw == "null" {
		return nil
	}

	var events []otlpEvent
	if err := json.Unmarshal([]byte(raw), &events); err != nil {
		return nil
	}

	result := make([]*tracepb.Span_Event, 0, len(events))
	for _, evt := range events {
		e := &tracepb.Span_Event{
			Name: evt.EventName,
		}
		if evt.Timestamp != "" {
			if t, err := time.Parse(time.RFC3339Nano, evt.Timestamp); err == nil {
				e.TimeUnixNano = uint64(t.UnixNano())
			}
		}
		if len(evt.EventAttributes) > 0 {
			e.Attributes = make([]*commonpb.KeyValue, 0, len(evt.EventAttributes))
			for k, v := range evt.EventAttributes {
				e.Attributes = append(e.Attributes, &commonpb.KeyValue{
					Key:   k,
					Value: toAnyValue(v),
				})
			}
		}
		result = append(result, e)
	}
	return result
}

// otlpLink is the JSON structure for a link stored in the Kusto Links column.
type otlpLink struct {
	TraceID            string                 `json:"TraceID"`
	SpanID             string                 `json:"SpanID"`
	TraceState         string                 `json:"TraceState"`
	SpanLinkAttributes map[string]interface{} `json:"SpanLinkAttributes"`
}

// parseLinks parses the Kusto Links JSON column into OTLP Span_Links.
func parseLinks(raw string) []*tracepb.Span_Link {
	if raw == "" || raw == "[]" || raw == "null" {
		return nil
	}

	var links []otlpLink
	if err := json.Unmarshal([]byte(raw), &links); err != nil {
		return nil
	}

	result := make([]*tracepb.Span_Link, 0, len(links))
	for _, l := range links {
		if l.TraceID == "" || l.SpanID == "" {
			continue
		}
		link := &tracepb.Span_Link{
			TraceId:    hexToBytes(l.TraceID),
			SpanId:     hexToBytes(l.SpanID),
			TraceState: l.TraceState,
		}
		if len(l.SpanLinkAttributes) > 0 {
			link.Attributes = make([]*commonpb.KeyValue, 0, len(l.SpanLinkAttributes))
			for k, v := range l.SpanLinkAttributes {
				link.Attributes = append(link.Attributes, &commonpb.KeyValue{
					Key:   k,
					Value: toAnyValue(v),
				})
			}
		}
		result = append(result, link)
	}
	return result
}

// convertOTLPSpanToProto converts a single Kusto otlpSpan row to an OTLP Span proto.
func convertOTLPSpanToProto(s *otlpSpan) *tracepb.Span {
	span := &tracepb.Span{
		TraceId:                hexToBytes(s.TraceID),
		SpanId:                 hexToBytes(s.SpanID),
		ParentSpanId:           hexToBytesOrNilIfZero(s.ParentID),
		Name:                   s.SpanName,
		Kind:                   spanKindFromString(s.SpanKind),
		StartTimeUnixNano:      toUnixNano(s.StartTime),
		EndTimeUnixNano:        toUnixNano(s.EndTime),
		Attributes:             parseAttributes(s.TraceAttributes),
		Events:                 parseEvents(s.Events),
		Links:                  parseLinks(s.Links),
		Status: &tracepb.Status{
			Code:    statusCodeFromString(s.SpanStatus),
			Message: s.SpanStatusMessage,
		},
	}
	return span
}

// groupSpansIntoTracesData groups a set of otlpSpan rows into TracesData messages,
// one per trace (as required by the V2 streaming API). Spans with the same
// service name share a Resource within a single ResourceSpans.
func groupSpansIntoTracesData(spans []*otlpSpan, logger hclog.Logger) []*tracepb.TracesData {
	// Group spans by traceID
	type traceGroup struct {
		traceID string
		spans   []*otlpSpan
	}

	traceMap := make(map[string]*traceGroup)
	var traceOrder []string

	for _, s := range spans {
		tg, ok := traceMap[s.TraceID]
		if !ok {
			tg = &traceGroup{traceID: s.TraceID}
			traceMap[s.TraceID] = tg
			traceOrder = append(traceOrder, s.TraceID)
		}
		tg.spans = append(tg.spans, s)
	}

	result := make([]*tracepb.TracesData, 0, len(traceOrder))
	for _, tid := range traceOrder {
		tg := traceMap[tid]
		td := buildTracesData(tg.spans, logger)
		if td != nil {
			result = append(result, td)
		}
	}

	return result
}

// buildTracesData creates a single TracesData for one trace, grouping spans by
// service name (ResourceAttributes) into separate ResourceSpans entries.
func buildTracesData(spans []*otlpSpan, logger hclog.Logger) *tracepb.TracesData {
	if len(spans) == 0 {
		return nil
	}

	// Group by service name (derived from ResourceAttributes)
	type resourceGroup struct {
		serviceName        string
		resourceAttributes string
		spans              []*tracepb.Span
	}

	groups := make(map[string]*resourceGroup)
	var groupOrder []string

	for _, s := range spans {
		svcName := extractServiceName(s.ResourceAttributes)
		key := svcName + "|" + s.ResourceAttributes

		rg, ok := groups[key]
		if !ok {
			rg = &resourceGroup{
				serviceName:        svcName,
				resourceAttributes: s.ResourceAttributes,
			}
			groups[key] = rg
			groupOrder = append(groupOrder, key)
		}
		rg.spans = append(rg.spans, convertOTLPSpanToProto(s))
	}

	td := &tracepb.TracesData{
		ResourceSpans: make([]*tracepb.ResourceSpans, 0, len(groupOrder)),
	}

	for _, key := range groupOrder {
		rg := groups[key]
		rs := &tracepb.ResourceSpans{
			Resource: &resourcepb.Resource{
				Attributes: parseAttributes(rg.resourceAttributes),
			},
			ScopeSpans: []*tracepb.ScopeSpans{
				{
					Spans: rg.spans,
				},
			},
		}
		td.ResourceSpans = append(td.ResourceSpans, rs)
	}

	return td
}

// extractServiceName pulls the service.name from a JSON ResourceAttributes string.
func extractServiceName(resourceAttrs string) string {
	if resourceAttrs == "" {
		return ""
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(resourceAttrs), &m); err != nil {
		return ""
	}
	if sn, ok := m["service.name"]; ok {
		return fmt.Sprint(sn)
	}
	return ""
}

// spanKindToJaegerString maps OTLP SpanKind strings to the lowercase Jaeger-style values
// used in the V2 proto Operation.span_kind field.
func spanKindToJaegerString(s string) string {
	switch s {
	case "SPAN_KIND_SERVER":
		return "server"
	case "SPAN_KIND_CLIENT":
		return "client"
	case "SPAN_KIND_CONSUMER":
		return "consumer"
	case "SPAN_KIND_PRODUCER":
		return "producer"
	case "SPAN_KIND_INTERNAL":
		return "internal"
	default:
		return strings.ToLower(strings.TrimPrefix(s, "SPAN_KIND_"))
	}
}
