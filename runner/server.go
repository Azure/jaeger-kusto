package runner

import (
	"github.com/dodopizza/jaeger-kusto/config"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"net"
)

func serveServer(c *config.PluginConfig, store shared.StoragePlugin, logger hclog.Logger) error {
	plugin := shared.StorageGRPCPlugin{
		Impl: store,
	}

	tracer, closer, err := config.NewPluginTracer(c)
	if err != nil {
		return err
	}
	defer closer.Close()

	server := newGRPCServerWithTracer(tracer)
	if err := plugin.GRPCServer(nil, server); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", c.RemoteAddress)
	if err != nil {
		return err
	}

	logger.Info("starting server on address", "address", listener.Addr())
	if err := server.Serve(listener); err != nil {
		return err
	}

	return nil
}
