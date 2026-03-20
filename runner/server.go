package runner

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/dodopizza/jaeger-kusto/config"
	collectortrace "github.com/dodopizza/jaeger-kusto/proto-gen/collector/trace/v1"
	storagev2 "github.com/dodopizza/jaeger-kusto/proto-gen/storage/v2"
	"github.com/dodopizza/jaeger-kusto/store"
	"github.com/hashicorp/go-hclog"
	"google.golang.org/grpc"
)

func serveServer(c *config.PluginConfig, v2store *store.V2Store, logger hclog.Logger) error {
	tracer, closer, err := config.NewPluginTracer(c)
	if err != nil {
		return err
	}
	defer closer.Close()

	server := newGRPCServerWithTracer(tracer)

	// Register V2 storage services
	storagev2.RegisterTraceReaderServer(server, v2store.TraceReader)
	storagev2.RegisterDependencyReaderServer(server, v2store.DependencyReader)
	collectortrace.RegisterTraceServiceServer(server, v2store.TraceWriter)

	scheme, address, err := parseListenAddress(c.RemoteListenAddress)
	if err != nil {
		return err
	}

	// perform cleanup for unix domain socket, before process exit
	if scheme == "unix" {
		defer os.Remove(address)
	}

	listener, err := net.Listen(scheme, address)
	if err != nil {
		return err
	}

	logger.Info("starting V2 remote storage server", "address", address, "scheme", scheme)
	wg := registerGracefulShutdown(server, logger)
	if err := server.Serve(listener); err != nil {
		return err
	}

	wg.Wait()
	return nil
}

func registerGracefulShutdown(server *grpc.Server, logger hclog.Logger) *sync.WaitGroup {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		sig := <-signals
		logger.Info("received signal, attempting gracefully stop server", "signal", sig)
		server.GracefulStop()
		logger.Info("server stopped")
		wg.Done()
	}()

	return wg
}

func parseListenAddress(addr string) (scheme, address string, err error) {
	u, err := url.Parse(addr)
	if err != nil {
		return "", "", err
	}

	proto := fmt.Sprintf("%s://", u.Scheme)

	return u.Scheme, strings.Replace(addr, proto, "", 1), nil
}
