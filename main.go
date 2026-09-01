// Command pager is a small self-hosted uptime monitor. It polls HTTP status
// endpoints, keeps all state in Redis, and pages a phone through a self-hosted
// ntfy server with thresholds, dedupe, ack and escalation.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Ginny-Binny/pager/internal/api"
	"github.com/Ginny-Binny/pager/internal/config"
	"github.com/Ginny-Binny/pager/internal/engine"
	"github.com/Ginny-Binny/pager/internal/notify"
	"github.com/Ginny-Binny/pager/internal/probe"
	"github.com/Ginny-Binny/pager/internal/store"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	slog.SetDefault(log)

	cfg, err := config.Load()
	if err != nil {
		log.Error("configuration error", "error", err)
		os.Exit(1)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	})
	defer rdb.Close()

	st := store.New(rdb)

	// Verify Redis up front. Every decision this process makes depends on it,
	// so failing loudly at boot beats discovering it during an outage.
	pingCtx, cancelPing := context.WithTimeout(context.Background(), 10*time.Second)
	err = st.Ping(pingCtx)
	cancelPing()
	if err != nil {
		log.Error("cannot reach redis", "addr", cfg.RedisAddr, "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng := engine.New(cfg, st, probe.New(),
		notify.New(cfg.NTFYURL, cfg.NTFYTopic, cfg.NTFYToken), log, nil)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           api.New(cfg, st, log, nil).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Info("starting pager",
		"checks", len(cfg.Checks),
		"listen", cfg.ListenAddr,
		"poll_interval", cfg.PollInterval.String(),
		"escalation_interval", cfg.EscalationInterval.String(),
		"failure_threshold", cfg.FailureThreshold,
		"recovery_threshold", cfg.RecoveryThreshold,
		"redis", cfg.RedisAddr,
		"ntfy_topic", cfg.NTFYTopic)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); eng.RunPoller(ctx) }()
	go func() { defer wg.Done(); eng.RunEscalation(ctx) }()

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	shutCtx, cancelShut := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShut()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Error("http shutdown failed", "error", err)
	}
	wg.Wait()
	log.Info("stopped")
}

func logLevel() slog.Level {
	switch os.Getenv("LOG_LEVEL") {
	case "debug", "DEBUG":
		return slog.LevelDebug
	case "warn", "WARN":
		return slog.LevelWarn
	case "error", "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
