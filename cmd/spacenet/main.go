// Command spacenet launches a standalone spacenet actor runtime node with all
// standard Skynet-compatible contrib modules enabled.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"

	"github.com/yuchanns/spacenet"
	"github.com/yuchanns/spacenet/contrib/all"
)

// version can be overridden at link time via -ldflags "-X main.version=v0.1.0".
var version = "dev"

func resolveVersion() string {
	if version != "" && version != "dev" {
		return version
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		return info.Main.Version
	}
	return version
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("spacenet", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		threads     = fs.Int("threads", 0, "number of actor worker goroutines (0 = NumCPU)")
		startSvc    = fs.String("start", "", "override start Lua service launched by bootstrap")
		baseDir     = fs.String("basedir", "", "base directory for resolving relative Lua paths")
		enableJIT   = fs.Bool("jit", true, "enable MoeLua native JIT tier on supported platforms")
		showVersion = fs.Bool("version", false, "print spacenet version and exit")
	)
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: spacenet [flags] [config.lua]\n\nFlags:\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}

	if *showVersion {
		fmt.Fprintf(stdout, "spacenet %s\n", resolveVersion())
		return 0
	}
	if *threads < 0 {
		fmt.Fprintf(stderr, "spacenet: -threads must be >= 0 (got %d)\n", *threads)
		return 2
	}
	if fs.NArg() > 1 {
		fmt.Fprintf(stderr, "spacenet: expected at most one config file, got %d\n", fs.NArg())
		return 2
	}

	opts := []spacenet.Option{
		spacenet.WithLogOutput(stdout),
		spacenet.WithContrib(all.Contribs()...),
	}
	if fs.NArg() == 1 {
		cfgOpts, err := loadConfigFile(fs.Arg(0))
		if err != nil {
			fmt.Fprintf(stderr, "spacenet: %v\n", err)
			return 1
		}
		opts = append(opts, cfgOpts...)
	}
	if *threads > 0 {
		opts = append(opts, spacenet.WithThreads(*threads))
	}
	if *startSvc != "" {
		opts = append(opts, spacenet.WithStart(*startSvc))
	}
	if *baseDir != "" {
		opts = append(opts, spacenet.WithBaseDir(*baseDir))
	}
	if *enableJIT {
		opts = append(opts, spacenet.WithJIT())
	}

	node, err := spacenet.New(opts...)
	if err != nil {
		fmt.Fprintf(stderr, "spacenet: init failed: %v\n", err)
		return 1
	}

	done := make(chan struct{})
	var abortWG sync.WaitGroup
	abortWG.Add(1)
	go func() {
		defer abortWG.Done()
		select {
		case <-ctx.Done():
			node.Abort()
		case <-done:
		}
	}()

	runErr := node.Run(ctx)
	close(done)
	abortWG.Wait()
	closeErr := node.Close()

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		fmt.Fprintf(stderr, "spacenet: %v\n", runErr)
		return 1
	}
	if closeErr != nil {
		fmt.Fprintf(stderr, "spacenet: close failed: %v\n", closeErr)
		return 1
	}
	return 0
}
