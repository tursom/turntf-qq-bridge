package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/tursom/turntf/app/turntf-qq-bridge/internal/qqbridge"
)

func main() {
	var (
		configPath  string
		checkConfig bool
	)

	flag.StringVar(&configPath, "config", "", "path to bridge config yaml")
	flag.StringVar(&configPath, "c", "", "path to bridge config yaml")
	flag.BoolVar(&checkConfig, "check-config", false, "validate config and exit")
	flag.Parse()

	if configPath == "" {
		fmt.Fprintln(os.Stderr, "missing required -config <path>")
		os.Exit(2)
	}

	cfg, err := qqbridge.LoadConfig(configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	if checkConfig {
		fmt.Fprintln(os.Stdout, "config ok")
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := log.New(os.Stderr, "", log.LstdFlags)
	if err := qqbridge.Run(ctx, cfg, logger); err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "run bridge: %v\n", err)
		os.Exit(1)
	}
}
