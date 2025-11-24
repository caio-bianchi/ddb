package main

import (
	"flag"
	"fmt"

	"labexame/ddb"
)

func main() {
	address := flag.String("address", "127.0.0.1:9000", "Coordinator RPC address")
	shards := flag.Int("shards", 2, "Number of shards")
	flag.Parse()

	fmt.Println("=== Coordinator ===")
	fmt.Println("Address:", *address)
	fmt.Println("Shards:", *shards)

	coord := ddb.NewCoordinator(*address, *shards)
	coord.Start()
}
