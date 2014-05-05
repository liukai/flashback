package main

import (
	"labix.org/v2/mgo"
	"log"
	"os"
	. "replay"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
)

func PanicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	// Will enable system threads to make sure all cpus can be well utilized.
	runtime.GOMAXPROCS(100)
	// TODO may need to switch to "flags" to make things looks cleaner.
	filename := os.Args[1]
	host := os.Args[2]
	parallel, err := strconv.Atoi(os.Args[3])
	PanicOnError(err)
	opSize, err := strconv.Atoi(os.Args[4])
	PanicOnError(err)

	// Prepare to dispatch ops
	var reader OpsReader
	var opsChan chan *Op
	useBestEffort := false
	if useBestEffort {
		err, reader = NewFileByLineOpsReader(filename)
		PanicOnError(err)
		opsChan = NewBestEffortOpsDispatcher(reader, opSize)
	} else {
		reader = NewCyclicOpsReader(func() OpsReader {
			err, reader := NewFileByLineOpsReader(filename)
			PanicOnError(err)
			return reader
		})
		opSize = 4294967295
		opsChan = NewByTimeOpsDispatcher(reader, opSize)
	}

	latencyChan := make(chan Latency, parallel)

	// Set up workers to do the job
	sampleRate := 0.001
	exit := make(chan int)
	opsExecuted := int64(0)
	fetch := func(id int, statsCollector IStatsCollector) {
		log.Printf("Worker #%d report for duty\n", id)
		session, err := mgo.Dial(host)
		PanicOnError(err)
		defer session.Close()
		exec := OpsExecutorWithStats(session, statsCollector)
		for {
			op := <-opsChan
			if op == nil {
				break
			}
			exec.Execute(op)
			atomic.AddInt64(&opsExecuted, 1)
		}
		exit <- 1
		log.Printf("Worker #%d done!\n", id)
	}
	statsCollectorList := make([]*StatsCollector, parallel)
	for i := 0; i < parallel; i++ {
		statsCollectorList[i] = NewStatsCollector()
		statsCollectorList[i].EnableLatencySampling(sampleRate, latencyChan)
		go fetch(i, statsCollectorList[i])
	}

	// Periodically report execution status
	go func() {
		statsAnalyser := NewStatsAnalyser(statsCollectorList, &opsExecuted,
			latencyChan, int(sampleRate*float64(opSize)))
		toFloat := func(nano int64) float64 {
			return float64(nano) / float64(1e6)
		}
		report := func() {
			status := statsAnalyser.GetStatus()
			log.Printf("Executed %d ops, %.2f ops/sec", opsExecuted,
				status.OpsPerSec)
			for _, opType := range AllOpTypes {
				allTime := status.AllTimeLatencies[opType]
				sinceLast := status.SinceLastLatencies[opType]
				log.Printf("  Op type: %s, count: %d, ops/sec: %.2f",
					opType, status.Counts[opType], status.TypeOpsSec[opType])
				template := "   %s: P50: %.2fms, P70: %.2fms, P90: %.2fms, " +
					"P95 %.2fms, P99 %.2fms, Max %.2fms\n"
				log.Printf(template, "Total", toFloat(allTime[P50]),
					toFloat(allTime[P70]), toFloat(allTime[P90]),
					toFloat(allTime[P95]), toFloat(allTime[P99]),
					toFloat(allTime[P100]))
				log.Printf(template, "Last ", toFloat(sinceLast[P50]),
					toFloat(sinceLast[P70]), toFloat(sinceLast[P90]),
					toFloat(sinceLast[P95]), toFloat(sinceLast[P99]),
					toFloat(sinceLast[P100]))
			}
		}
		defer report()

		for opsExecuted < int64(opSize) {
			time.Sleep(5 * time.Second)
			report()
		}
	}()

	// Wait for workers
	received := 0
	for received < parallel {
		<-exit
		received += 1
	}
}
