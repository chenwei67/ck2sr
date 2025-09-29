package main

import (
	"fmt"
	"os"

	"github.com/sunkaimr/ck2sr/cmd/dtool/commands"
)

const version = "1.0.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]
	switch command {
	case "generate_data":
		if err := commands.RunGenerateData(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
	case "version", "--version", "-v":
		fmt.Printf("dtool version %s\n", version)
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Printf(`dtool - Data Tool for ck2sr debugging and testing

Version: %s

Usage:
  dtool <command> [options]

Commands:
  generate_data    Generate test data for StarRocks or ClickHouse
  version          Show version information
  help             Show this help message

Examples:
  # Generate StarRocks test data
  dtool generate_data starrocks --sql_endpoint "10.192.31.3" --sql_port 9030 --sql_auth_username "root" --sql_auth_password "password" --db test --table table1

  # Generate ClickHouse test data
  dtool generate_data clickhouse --sql_endpoint "10.192.31.3" --sql_port 9030 --sql_auth_username "default" --sql_auth_password "" --db test --table table1

For more information on a specific command, use:
  dtool <command> --help

`, version)
}