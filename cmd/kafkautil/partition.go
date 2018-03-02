package main

import (
	"fmt"
	"strconv"

	"github.com/burdiyan/kafkautil"
	"github.com/spf13/cobra"
)

var partitionCmd = &cobra.Command{
	Use:   "partition <value> <total-partitions> [flags]",
	Short: "Get partition number for some value",
	RunE: func(cmd *cobra.Command, args []string) error {
		h := kafkautil.MurmurHasher()

		if _, err := h.Write([]byte(args[0])); err != nil {
			return fmt.Errorf("unable to write hash for %s: %v", args[0], err)
		}

		p, err := strconv.Atoi(args[1])
		if err != nil {
			return fmt.Errorf("invalid total partitions: %v", err)
		}

		fmt.Println(int(h.Sum32()) % int(p))

		return nil
	},
}

func init() {
	rootCmd.AddCommand(partitionCmd)
}
