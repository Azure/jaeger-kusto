package runner

import (
	"github.com/dodopizza/jaeger-kusto/config"
	"github.com/dodopizza/jaeger-kusto/store"
	ot "github.com/grpc-ecosystem/grpc-opentracing/go/otgrpc"
	"github.com/hashicorp/go-hclog"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
)

// Serve starts the V2 gRPC remote storage server.
func Serve(c *config.PluginConfig, v2store *store.V2Store, logger hclog.Logger) error {
	return serveServer(c, v2store, logger)
}

func newGRPCServerWithTracer(tracer opentracing.Tracer) *grpc.Server {
	return grpc.NewServer(
		grpc.UnaryInterceptor(ot.OpenTracingServerInterceptor(tracer)),
		grpc.StreamInterceptor(ot.OpenTracingStreamServerInterceptor(tracer)),
	)
}
