package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, generated, err := LoadConfig(configFilename)
	if err != nil {
		appLog.Error("startup failed while loading config", "error", err)
		os.Exit(1)
	}
	if generated {
		appLog.Info("configuration created; edit it and restart", "file", configFilename)
		return
	}

	service, err := NewService(cfg)
	if err != nil {
		appLog.Error("startup failed while creating service", "error", err)
		os.Exit(1)
	}
	if err := service.Init(); err != nil {
		appLog.Error("startup failed while initializing service", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	service.StartWorkers(ctx)

	napcat := NewNapcatClient(cfg)
	bot := NewBot(service, napcat, cfg)
	runErr := make(chan error, 1)
	go func() {
		runErr <- napcat.Run(ctx, bot.HandleMessage, bot.HandleFriendRequest, bot.SetLoginInfo)
	}()

	select {
	case <-ctx.Done():
		appLog.Info("shutdown signal received")
	case err := <-runErr:
		if err != nil {
			appLog.Error("bot stopped with error", "error", err)
		} else {
			appLog.Info("bot stopped")
		}
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := service.Shutdown(shutdownCtx); err != nil {
		appLog.Error("shutdown failed", "error", err)
	}
}
