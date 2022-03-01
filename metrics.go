// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net"
	"net/http"
	"time"
)

var (
	attemptBuckets = []float64{
		1., 2., 3., 4., 5., 6., 7., 8., 9.,
		10, 20, 30, 40, 50, 60, 70, 80, 90,
	}

	latencyBuckets = []float64{
		.001, .002, .003, .004, .005, .006, .007, .008, .009,
		.01, .02, .03, .04, .05, .06, .07, .08, .09,
		0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9,
		1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
		10., 20., 30., 40., 50., 60.,
	}

	stmtStats = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stmt_stats",
	}, []string{"lio"})
)

func metricsServer(ctx context.Context, db *pgxpool.Pool) error {

	prometheus.Unregister(collectors.NewGoCollector())

	l, err := net.Listen("tcp", *MetricsServerPort)
	if err != nil {
		return errors.Wrap(err, "opening port")
	}
	log.Printf("listening on %s", l.Addr())
	srv := http.Server{
		Handler: promhttp.Handler(),
	}
	go func() {
		<-ctx.Done()
		grace, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = srv.Shutdown(grace)
	}()
	return srv.Serve(l)
}
