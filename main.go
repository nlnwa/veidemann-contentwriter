package main

import (
	"fmt"
	"github.com/nlnwa/veidemann-contentwriter/database"
	"github.com/nlnwa/veidemann-contentwriter/logger"
	"github.com/nlnwa/veidemann-contentwriter/server"
	"github.com/nlnwa/veidemann-contentwriter/settings"
	"github.com/nlnwa/veidemann-contentwriter/telemetry"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"strings"
)

func main() {
	pflag.String("interface", "", "interface the browser controller api listens to. No value means all interfaces.")
	pflag.Int("port", 8080, "port the browser controller api listens to.")
	pflag.String("host-name", "", "")
	pflag.String("warc-dir", "", "")
	pflag.Int("warc-writer-pool-size", 1, "")
	pflag.String("work-dir", "", "")
	pflag.Int("termination-grace-period-seconds", 0, "")

	pflag.String("db-host", "rethinkdb-proxy", "DB host")
	pflag.Int("db-port", 28015, "DB port")
	pflag.String("db-name", "veidemann", "DB name")
	pflag.String("db-user", "", "Database username")
	pflag.String("db-password", "", "Database password")
	pflag.Duration("db-query-timeout", 1*time.Minute, "Database query timeout")
	pflag.Int("db-max-retries", 5, "Max retries when database query fails")
	pflag.Int("db-max-open-conn", 10, "Max open database connections")
	pflag.Bool("db-use-opentracing", false, "Use opentracing for database queries")
	pflag.Duration("db-cache-ttl", 5*time.Minute, "How long to cache results from database")

	pflag.String("metrics-interface", "", "Interface for exposing metrics. Empty means all interfaces")
	pflag.Int("metrics-port", 9153, "Port for exposing metrics")
	pflag.String("metrics-path", "/metrics", "Path for exposing metrics")

	pflag.String("log-level", "info", "log level, available levels are panic, fatal, error, warn, info, debug and trace")
	pflag.String("log-formatter", "logfmt", "log formatter, available values are logfmt and json")
	pflag.Bool("log-method", false, "log method names")

	pflag.Parse()
	_ = viper.BindPFlags(pflag.CommandLine)

	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv()
	err := viper.BindPFlags(pflag.CommandLine)
	if err != nil {
		log.Fatal().Err(err).Msg("Could not parse flags")
	}

	logger.InitLog(viper.GetString("log-level"), viper.GetString("log-formatter"), viper.GetBool("log-method"))

	db := database.NewRethinkDbConnection(
		database.Options{
			Address:            fmt.Sprintf("%s:%d", viper.GetString("db-host"), viper.GetInt("db-port")),
			Username:           viper.GetString("db-user"),
			Password:           viper.GetString("db-password"),
			Database:           viper.GetString("db-name"),
			QueryTimeout:       viper.GetDuration("db-query-timeout"),
			MaxOpenConnections: viper.GetInt("db-max-open-conn"),
			MaxRetries:         viper.GetInt("db-max-retries"),
			UseOpenTracing:     viper.GetBool("db-use-opentracing"),
		},
	)
	if err := db.Connect(); err != nil {
		panic(err)
	}
	defer db.Close()

	configCache := database.NewConfigCache(db, viper.GetDuration("db-cache-ttl"))
	contentwriterService := server.New(viper.GetString("interface"), viper.GetInt("port"), settings.ViperSettings{}, configCache)

	// telemetry setup
	tracer, closer := telemetry.InitTracer("Scope checker")
	if tracer != nil {
		opentracing.SetGlobalTracer(tracer)
		defer closer.Close()
	}

	errc := make(chan error, 1)

	ms := telemetry.NewMetricsServer(viper.GetString("metrics-interface"), viper.GetInt("metrics-port"), viper.GetString("metrics-path"))
	go func() { errc <- ms.Start() }()
	defer ms.Close()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

		select {
		case err := <-errc:
			log.Err(err).Msg("Metrics server failed")
			contentwriterService.Shutdown()
		case sig := <-signals:
			log.Debug().Msgf("Received signal: %s", sig)
			contentwriterService.Shutdown()
		}
	}()

	err = contentwriterService.Start()
	if err != nil {
		log.Err(err).Msg("Could not start Content writer service")
	}
}
