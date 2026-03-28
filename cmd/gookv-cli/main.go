package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/ryogrid/gookv/pkg/client"
)

const version = "0.1.0"

func main() {
	pd := flag.String("pd", "127.0.0.1:2379", "PD server address(es), comma-separated")
	batch := flag.String("c", "", "Execute statement(s) and exit")
	hexMode := flag.Bool("hex", false, "Start in hex display mode")
	showVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("gookv-cli v%s (go%s, %s/%s)\n", version, runtime.Version()[2:], runtime.GOOS, runtime.GOARCH)
		os.Exit(0)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	endpoints := splitEndpoints(*pd)

	c, err := client.NewClient(ctx, client.Config{
		PDAddrs: endpoints,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: connect to PD: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = c.Close() }()

	exec := NewExecutor(c)
	fmtr := NewFormatter(os.Stdout)
	fmtr.SetErrOut(os.Stderr)

	// Set default stderr for executor warnings
	defaultStderr = os.Stderr

	if *hexMode {
		fmtr.SetDisplayMode(DisplayHex)
	}

	if *batch != "" {
		os.Exit(runBatch(ctx, exec, fmtr, *batch))
	}

	if !isTerminal() {
		os.Exit(runPipe(ctx, exec, fmtr))
	}

	pdAddr := strings.Join(endpoints, ",")
	os.Exit(runREPL(ctx, exec, fmtr, pdAddr))
}

func splitEndpoints(s string) []string {
	parts := strings.Split(s, ",")
	var result []string
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
