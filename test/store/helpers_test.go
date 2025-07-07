package store

import (
	"github.com/Azure/jaeger-kusto/config"
)

var (
	testPluginConfig = NewTestPluginConfig()
)

func NewTestPluginConfig() *config.PluginConfig {
	pc := config.NewDefaultPluginConfig()
	// override values for testing purpose
	pc.KustoConfigPath = "jaeger-kusto-config.json"
	pc.LogLevel = "debug"
	pc.WriterWorkersCount = 1
	return pc
}
