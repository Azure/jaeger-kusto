package runner

import (
	"github.com/dodopizza/jaeger-kusto/config"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"google.golang.org/grpc/health"
)

func servePlugin(c *config.PluginConfig, store shared.StoragePlugin, logger hclog.Logger) error {
	tracer, closer, err := config.NewPluginTracer(c)
	if err != nil {
		return err
	}
	defer closer.Close()

	handler := shared.NewGRPCHandlerWithPlugins(store, nil, nil)
	server := newGRPCServerWithTracer(tracer)
	hs := health.NewServer()
	if err := handler.Register(server, hs); err != nil {
		return err
	}

	logger.Info("starting plugin")
	return nil
}
