package main

import (
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "kafkautil",
	Short: "Kafka Utilities",
}

func main() {
	rootCmd.Execute()
}
