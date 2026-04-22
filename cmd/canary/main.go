package main

import (
	"flag"
	"fmt"
	"os"
)

var version = "0.1.0-dev"

func main() {
	var (
		cfgPath     = flag.String("config", "canary.yaml", "path to YAML config file")
		showVersion = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVersion {
		fmt.Println("canary", version)
		return
	}

	if _, err := os.Stat(*cfgPath); err != nil {
		fmt.Fprintf(os.Stderr, "canary: config %q: %v\n", *cfgPath, err)
		os.Exit(2)
	}

	fmt.Fprintln(os.Stderr, "canary: scaffold only; wiring lands in later commits")
}
