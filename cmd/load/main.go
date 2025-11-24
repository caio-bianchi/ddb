package main

import (
	"bufio"
	"flag"
	"fmt"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"labexame/ddb"
)

var coordAddr string

func main() {
	flag.StringVar(&coordAddr, "coord", "127.0.0.1:9000", "coordinator address")
	flag.Parse()

	fmt.Println("=== Distributed DB Benchmark Tool ===")
	fmt.Println("Coordinator:", coordAddr)

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("bench> ")
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		args := strings.Fields(line)
		cmd := args[0]

		switch cmd {

		case "bench":
			runSerialBenchmark(args[1:])

		case "benchc":
			runConcurrentBenchmark(args[1:])

		case "mix":
			runMixedBenchmark(args[1:])

		case "exit":
			return

		default:
			fmt.Println("unk cmd:", cmd)
		}
	}

}

///////////////////////////////////////////////////////////////////////////////
// BENCH STRUCTS
///////////////////////////////////////////////////////////////////////////////

type BenchResult struct {
	Latencies []time.Duration
	Errors    int
}

///////////////////////////////////////////////////////////////////////////////
// SINGLE OP
///////////////////////////////////////////////////////////////////////////////

func benchOp(op, key, value string) (time.Duration, error) {
	start := time.Now()

	cli, err := rpc.Dial("tcp", coordAddr)
	if err != nil {
		return 0, err
	}
	defer cli.Close()

	switch op {

	case "put":
		args := &ddb.ClientPutArgs{Key: key, Value: value}
		reply := &ddb.ClientPutReply{}
		err = cli.Call("Coordinator.RPCPut", args, reply)
		if reply.Err != "" {
			err = fmt.Errorf(reply.Err)
		}

	case "get":
		args := &ddb.ClientGetArgs{Key: key}
		reply := &ddb.ClientGetReply{}
		err = cli.Call("Coordinator.RPCGet", args, reply)
		if reply.Err != "" {
			err = fmt.Errorf(reply.Err)
		}

	case "del":
		args := &ddb.ClientDeleteArgs{Key: key}
		reply := &ddb.ClientDeleteReply{}
		err = cli.Call("Coordinator.RPCDelete", args, reply)
		if reply.Err != "" {
			err = fmt.Errorf(reply.Err)
		}
	}

	return time.Since(start), err
}

///////////////////////////////////////////////////////////////////////////////
// SERIAL BENCH
///////////////////////////////////////////////////////////////////////////////

func runSerialBenchmark(args []string) {
	if len(args) < 2 {
		fmt.Println("usage: bench <op> <count>")
		return
	}

	op := args[0]
	count, _ := strconv.Atoi(args[1])

	fmt.Printf("Running SERIAL benchmark: %d %s ops...\n", count, op)

	res := BenchResult{}

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("k%d", i)

		lat, err := benchOp(op, key, "v")
		if err != nil {
			res.Errors++
		} else {
			res.Latencies = append(res.Latencies, lat)
		}
	}

	printBenchmark(res)
}

///////////////////////////////////////////////////////////////////////////////
// CONCURRENT BENCH
///////////////////////////////////////////////////////////////////////////////

func runConcurrentBenchmark(args []string) {
	if len(args) < 3 {
		fmt.Println("usage: benchc <op> <count> <goroutines>")
		return
	}

	op := args[0]
	count, _ := strconv.Atoi(args[1])
	g, _ := strconv.Atoi(args[2])

	fmt.Printf("Running CONCURRENT benchmark: %d ops with %d goroutines...\n",
		count, g)

	res := BenchResult{}
	var wg sync.WaitGroup
	var mu sync.Mutex

	perG := count / g

	for i := 0; i < g; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < perG; j++ {
				key := fmt.Sprintf("k%d_%d", id, j)

				lat, err := benchOp(op, key, "v")
				mu.Lock()
				if err != nil {
					res.Errors++
				} else {
					res.Latencies = append(res.Latencies, lat)
				}
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	printBenchmark(res)
}

///////////////////////////////////////////////////////////////////////////////
// MIXED WORKLOAD: ratio% GET, (100-ratio)% PUT
///////////////////////////////////////////////////////////////////////////////

func runMixedBenchmark(args []string) {
	if len(args) < 3 {
		fmt.Println("usage: mix <count> <goroutines> <ratioGET>")
		return
	}

	count, _ := strconv.Atoi(args[0])
	g, _ := strconv.Atoi(args[1])
	ratioGet, _ := strconv.Atoi(args[2])

	fmt.Printf("Running MIXED benchmark: %d ops, %d goroutines, GET=%d%%\n",
		count, g, ratioGet)

	res := BenchResult{}
	var wg sync.WaitGroup
	var mu sync.Mutex

	perG := count / g

	for i := 0; i < g; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			for j := 0; j < perG; j++ {

				useGet := rand.Intn(100) < ratioGet
				var op string
				if useGet {
					op = "get"
				} else {
					op = "put"
				}

				key := fmt.Sprintf("m%d_%d", id, j)

				lat, err := benchOp(op, key, "v")
				mu.Lock()
				if err != nil {
					res.Errors++
				} else {
					res.Latencies = append(res.Latencies, lat)
				}
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()
	printBenchmark(res)
}

///////////////////////////////////////////////////////////////////////////////
// REPORT
///////////////////////////////////////////////////////////////////////////////

func printBenchmark(res BenchResult) {
	if len(res.Latencies) == 0 {
		fmt.Println("No successful operations")
		return
	}

	sort.Slice(res.Latencies, func(i, j int) bool {
		return res.Latencies[i] < res.Latencies[j]
	})

	total := len(res.Latencies)
	sum := time.Duration(0)
	for _, l := range res.Latencies {
		sum += l
	}

	avg := sum / time.Duration(total)

	p50 := res.Latencies[int(0.50*float64(total))]
	p95 := res.Latencies[int(0.95*float64(total))]
	p99 := res.Latencies[int(0.99*float64(total))]

	fmt.Println("\n--- BENCHMARK RESULTS ---")
	fmt.Println("Successful ops:", total)
	fmt.Println("Errors:", res.Errors)
	fmt.Println("Avg latency:", avg)
	fmt.Println("P50:", p50)
	fmt.Println("P95:", p95)
	fmt.Println("P99:", p99)
	fmt.Println("------------------------\n")
}
