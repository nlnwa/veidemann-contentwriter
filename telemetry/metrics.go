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
	"github.com/prometheus/client_golang/prometheus"
)

var (
	CanonicalizationsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNs,
		Subsystem: metricsSubsystem,
		Name:      "canonicalizations_total",
		Help:      "Total URIs canonicalized",
	})

	ScopechecksTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: metricsNs,
		Subsystem: metricsSubsystem,
		Name:      "scopechecks_total",
		Help:      "Total URIs checked for scope inclusion",
	})

	ScopecheckResponseTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: metricsNs,
		Subsystem: metricsSubsystem,
		Name:      "scopecheck_response_total",
		Help:      "Total scopecheck responses for each response code",
	},
		[]string{"code"},
	)

	CompileScriptSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNs,
		Subsystem: metricsSubsystem,
		Name:      "script_compile_seconds",
		Help:      "Time for compiling a script in seconds",
		Buckets:   []float64{.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 20, 30, 40, 50, 60, 120, 180, 240},
	})

	ExecuteScriptSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: metricsNs,
		Subsystem: metricsSubsystem,
		Name:      "script_execute_seconds",
		Help:      "Time for executing a script in seconds",
		Buckets:   []float64{.005, .01, .025, .05, .075, .1, .25, .5, .75, 1, 2.5, 5, 7.5, 10, 20, 30, 40, 50, 60, 120, 180, 240},
	})
)

const (
	metricsNs        = "veidemann"
	metricsSubsystem = "scopeservice"
)
