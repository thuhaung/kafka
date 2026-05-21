package main

import (
	"log"

	"github.com/thuhaung/kafka/internal/cli"
)

func main() {
	if err := cli.NewRootCommand().Execute(); err != nil {
		log.Fatal(err)
	}
}
