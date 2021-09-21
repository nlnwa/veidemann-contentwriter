/*
 * Copyright 2020 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package telemetry

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"net/http"
	"sync"
	"time"
)

var once sync.Once

// MetricsServer is the Prometheus metrics endpoint for the Browser Controller
type MetricsServer struct {
	addr   string
	path   string
	server *http.Server
}

// NewMetricsServer returns a new instance of MetricsServer listening on the given port
func NewMetricsServer(listenInterface string, listenPort int, path string) *MetricsServer {
	a := &MetricsServer{
		addr: fmt.Sprintf("%s:%d", listenInterface, listenPort),
		path: path,
	}
	once.Do(func() {
		prometheus.MustRegister(
			CanonicalizationsTotal,
			ScopechecksTotal,
			ScopecheckResponseTotal,
			CompileScriptSeconds,
			ExecuteScriptSeconds,
			collectors.NewBuildInfoCollector(),
		)
	})

	return a
}

func (a *MetricsServer) Start() error {
	router := http.NewServeMux()
	router.Handle(a.path, promhttp.Handler())

	a.server = &http.Server{
		Addr:         a.addr,
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  5 * time.Second,
	}

	log.Info().Msgf("Metrics server listening on address: %s", a.addr)
	err := a.server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("failed to listen on %s: %w", a.addr, err)
	}
	return nil
}

func (a *MetricsServer) Close() {
	log.Info().Msgf("Shutting down Metrics server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a.server.SetKeepAlivesEnabled(false)
	_ = a.server.Shutdown(ctx)
}
