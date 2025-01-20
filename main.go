package main

import (
	"context"
	"fmt"
	"os"

	"github.com/igomez10/mongostream-cli/commands/stream"
	"github.com/urfave/cli/v3"
)

func main() {
	ctx := context.Background()
	cmd := &cli.Command{
		Name:                  "mongostream-cli",
		EnableShellCompletion: true,
		Usage:                 "CLI for MongoDB Change Streams",
		ExitErrHandler: func(ctx context.Context, c *cli.Command, err error) {
			fmt.Printf("Error: %v\n", err)
			os.Exit(1)
		},
		Commands: []*cli.Command{
			stream.GetCmd(),
		},
	}

	if err := cmd.Run(ctx, os.Args); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
