package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"

	"labexame/ddb"
)

type OpResult struct {
	Index      int
	Op         string
	Latency    time.Duration
	Ok         bool
	ErrMessage string
}

func main() {
	rand.Seed(time.Now().UnixNano())

	coordAddr := flag.String("coord", "127.0.0.1:9000", "Coordinator address (ip:port)")
	opMode := flag.String("op", "put", "Operation mode: put | get | del | mix")
	totalOps := flag.Int("count", 10000, "Total number of operations")
	concurrency := flag.Int("conc", 20, "Number of concurrent workers")
	csvPath := flag.String("csv", "metrics.csv", "Path to CSV output file")
	flag.Parse()

	fmt.Println("=== Distributed DB Metrics Benchmark ===")
	fmt.Println("Coordinator:", *coordAddr)
	fmt.Println("Op mode   :", *opMode)
	fmt.Println("Ops count :", *totalOps)
	fmt.Println("Workers   :", *concurrency)
	fmt.Println("CSV output:", *csvPath)
	fmt.Println("========================================")

	// Gera conjunto de chaves para o benchmark
	keys := make([]string, *totalOps)
	for i := 0; i < *totalOps; i++ {
		// pode ser qualquer padrão, o importante é ser determinístico
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	// Se o modo envolve GET/DEL, precisamos popular o cluster antes
	switch *opMode {
	case "get", "del", "mix":
		fmt.Println("[warmup] Populando o cluster com PUTs iniciais...")
		if err := warmupPut(*coordAddr, keys); err != nil {
			log.Fatalf("Erro no warmup PUT: %v", err)
		}
		fmt.Println("[warmup] Concluído.")
	}

	results, benchDuration := runBenchmark(*coordAddr, *opMode, keys, *concurrency)

	fmt.Println("\n=== Resultados ===")
	computeAndPrintStats(results, benchDuration)

	if err := writeCSV(*csvPath, results); err != nil {
		log.Printf("Erro ao escrever CSV: %v", err)
	} else {
		fmt.Printf("CSV salvo em: %s\n", *csvPath)
	}
}

// ----------------------------------------------------------------------
// Warmup: faz PUT em todas as chaves para preparar GET/DEL/MIX
// ----------------------------------------------------------------------
func warmupPut(coordAddr string, keys []string) error {
	client, err := rpc.Dial("tcp", coordAddr)
	if err != nil {
		return fmt.Errorf("dial coordinator: %v", err)
	}
	defer client.Close()

	for _, k := range keys {
		args := &ddb.ClientPutArgs{Key: k, Value: "warmup"}
		reply := &ddb.ClientPutReply{}
		if err := client.Call("Coordinator.RPCPut", args, reply); err != nil {
			return fmt.Errorf("RPCPut warmup error: %v", err)
		}
		if reply.Err != "" {
			return fmt.Errorf("RPCPut warmup logical error: %s", reply.Err)
		}
	}
	return nil
}

// ----------------------------------------------------------------------
// Benchmark principal
// ----------------------------------------------------------------------
func runBenchmark(coordAddr, opMode string, keys []string, concurrency int) ([]OpResult, time.Duration) {
	totalOps := len(keys)

	jobs := make(chan int, totalOps)
	resultsCh := make(chan OpResult, totalOps)

	var wg sync.WaitGroup

	startBench := time.Now()

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			cli, err := rpc.Dial("tcp", coordAddr)
			if err != nil {
				log.Printf("[worker %d] erro ao conectar no coordinator: %v", workerID, err)
				// Se não conecta, esse worker não contribui
				return
			}
			defer cli.Close()

			for idx := range jobs {
				key := keys[idx]
				op := opMode
				// No modo mix, intercalamos PUT e GET (50/50)
				if opMode == "mix" {
					if idx%2 == 0 {
						op = "put"
					} else {
						op = "get"
					}
				}

				res := doOperation(cli, op, key, idx)
				resultsCh <- res
			}
		}(w)
	}

	// Distribui jobs
	for i := 0; i < totalOps; i++ {
		jobs <- i
	}
	close(jobs)

	wg.Wait()
	close(resultsCh)

	endBench := time.Now()
	benchDuration := endBench.Sub(startBench)

	// Coleta resultados em slice
	results := make([]OpResult, 0, totalOps)
	for r := range resultsCh {
		results = append(results, r)
	}

	return results, benchDuration
}

// Executa uma única operação (PUT/GET/DEL) e mede a latência
func doOperation(cli *rpc.Client, op, key string, idx int) OpResult {
	start := time.Now()
	res := OpResult{
		Index: idx,
		Op:    op,
		Ok:    false,
	}

	switch op {
	case "put":
		args := &ddb.ClientPutArgs{Key: key, Value: "v"}
		reply := &ddb.ClientPutReply{}
		err := cli.Call("Coordinator.RPCPut", args, reply)
		res.Latency = time.Since(start)

		if err != nil {
			res.ErrMessage = err.Error()
			return res
		}
		if reply.Err != "" {
			res.ErrMessage = reply.Err
			return res
		}
		res.Ok = true

	case "get":
		args := &ddb.ClientGetArgs{Key: key}
		reply := &ddb.ClientGetReply{}
		err := cli.Call("Coordinator.RPCGet", args, reply)
		res.Latency = time.Since(start)

		if err != nil {
			res.ErrMessage = err.Error()
			return res
		}
		if reply.Err != "" {
			res.ErrMessage = reply.Err
			return res
		}
		// mesmo que não encontre (Found=false), consideramos sucesso de RPC
		res.Ok = true

	case "del", "delete":
		args := &ddb.ClientDeleteArgs{Key: key}
		reply := &ddb.ClientDeleteReply{}
		err := cli.Call("Coordinator.RPCDelete", args, reply)
		res.Latency = time.Since(start)

		if err != nil {
			res.ErrMessage = err.Error()
			return res
		}
		if reply.Err != "" {
			res.ErrMessage = reply.Err
			return res
		}
		res.Ok = true

	default:
		res.Latency = time.Since(start)
		res.ErrMessage = fmt.Sprintf("unknown op: %s", op)
	}

	return res
}

// ----------------------------------------------------------------------
// Estatísticas (latência e throughput)
// ----------------------------------------------------------------------
func computeAndPrintStats(results []OpResult, benchDuration time.Duration) {
	if len(results) == 0 {
		fmt.Println("Nenhum resultado coletado.")
		return
	}

	var latencies []time.Duration
	var okCount int
	var putCount, getCount, delCount int

	for _, r := range results {
		if r.Ok {
			okCount++
		}
		latencies = append(latencies, r.Latency)

		switch r.Op {
		case "put":
			putCount++
		case "get":
			getCount++
		case "del", "delete":
			delCount++
		}
	}

	totalOps := len(results)
	totalSeconds := benchDuration.Seconds()
	throughput := float64(totalOps) / totalSeconds

	// Ordena latências para percentis
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	avg := averageDuration(latencies)
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)
	max := latencies[len(latencies)-1]

	fmt.Printf("Total ops   : %d\n", totalOps)
	fmt.Printf("Sucesso     : %d (%.2f%%)\n", okCount, 100.0*float64(okCount)/float64(totalOps))
	fmt.Printf("PUT count   : %d\n", putCount)
	fmt.Printf("GET count   : %d\n", getCount)
	fmt.Printf("DEL count   : %d\n", delCount)
	fmt.Printf("Duração     : %.3fs\n", totalSeconds)
	fmt.Printf("Throughput  : %.2f ops/s\n", throughput)

	fmt.Println("\n--- Latência (ms) ---")
	fmt.Printf("média : %.3f ms\n", ms(avg))
	fmt.Printf("p50   : %.3f ms\n", ms(p50))
	fmt.Printf("p95   : %.3f ms\n", ms(p95))
	fmt.Printf("p99   : %.3f ms\n", ms(p99))
	fmt.Printf("max   : %.3f ms\n", ms(max))
}

func averageDuration(ds []time.Duration) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	var sum time.Duration
	for _, d := range ds {
		sum += d
	}
	return time.Duration(int64(sum) / int64(len(ds)))
}

func percentile(ds []time.Duration, p float64) time.Duration {
	if len(ds) == 0 {
		return 0
	}
	if p <= 0 {
		return ds[0]
	}
	if p >= 1 {
		return ds[len(ds)-1]
	}
	// índice baseado em 0
	index := int(math.Ceil(p*float64(len(ds)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(ds) {
		index = len(ds) - 1
	}
	return ds[index]
}

func ms(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

// ----------------------------------------------------------------------
// CSV
// ----------------------------------------------------------------------
func writeCSV(path string, results []OpResult) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Cabeçalho
	if err := w.Write([]string{"index", "op", "latency_us", "ok", "error"}); err != nil {
		return err
	}

	for _, r := range results {
		row := []string{
			fmt.Sprintf("%d", r.Index),
			r.Op,
			fmt.Sprintf("%d", r.Latency.Microseconds()),
			fmt.Sprintf("%t", r.Ok),
			r.ErrMessage,
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}
