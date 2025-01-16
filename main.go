package main

import (
	"context"
	"fmt"
	"os"

	"github.com/igomez10/mongocli/commands/stream"
	"github.com/urfave/cli/v3"
)

func main() {
	ctx := context.Background()
	cmd := &cli.Command{
		Name:                  "mongocli",
		EnableShellCompletion: true,
		Usage:                 "CLI for MongoDB",
		ExitErrHandler: func(ctx context.Context, c *cli.Command, err error) {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		},
		Commands: []*cli.Command{
			stream.GetCmd(),
		},
	}

	cmd.Run(ctx, os.Args)
}
