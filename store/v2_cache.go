package store

import (
	"context"
	"time"

	"github.com/hashicorp/go-hclog"
)

// dependencyRefresherV2 runs a background goroutine that periodically refreshes
// the dependency cache for the V2 reader.
type dependencyRefresherV2 struct {
	reader   *kustoV2Reader
	cache    *discoveryCache
	lookback time.Duration
	interval time.Duration
	logger   hclog.Logger
	stopCh   chan struct{}
}

func newDependencyRefresherV2(reader *kustoV2Reader, cache *discoveryCache, interval time.Duration, logger hclog.Logger) *dependencyRefresherV2 {
	return &dependencyRefresherV2{
		reader:   reader,
		cache:    cache,
		lookback: maxDependencyLookback,
		interval: interval,
		logger:   logger,
		stopCh:   make(chan struct{}),
	}
}

func (d *dependencyRefresherV2) start() {
	go func() {
		d.logger.Info("V2 Dependency cache refresher started", "interval", d.interval, "lookback", d.lookback)

		ticker := time.NewTicker(d.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.refresh()
			case <-d.stopCh:
				d.logger.Info("V2 Dependency cache refresher stopped")
				return
			}
		}
	}()
}

func (d *dependencyRefresherV2) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	endTs := time.Now()
	startTs := endTs.Add(-d.lookback)
	deps, err := d.reader.fetchDependencies(ctx, startTs, endTs)
	if err != nil {
		d.logger.Error("V2 Background dependency refresh failed", "error", err)
		return
	}

	d.cache.set(dependencyCacheKey, deps)
	d.logger.Info("V2 Background dependency refresh complete", "links", len(deps))
}

func (d *dependencyRefresherV2) stop() {
	close(d.stopCh)
}
