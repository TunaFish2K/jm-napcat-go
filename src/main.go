package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, generated, err := LoadConfig(configFilename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Startup failed: %v\n", err)
		os.Exit(1)
	}
	if generated {
		fmt.Printf("Created %s. Edit it and run the program again.\n", configFilename)
		return
	}

	service, err := NewService(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Startup failed: %v\n", err)
		os.Exit(1)
	}
	if err := service.Init(); err != nil {
		fmt.Fprintf(os.Stderr, "Startup failed: %v\n", err)
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
		fmt.Println("Shutting down gracefully...")
	case err := <-runErr:
		if err != nil {
			fmt.Fprintf(os.Stderr, "Bot failed: %v\n", err)
		}
		stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := service.Shutdown(shutdownCtx); err != nil {
		fmt.Fprintf(os.Stderr, "Shutdown failed: %v\n", err)
	}
}
