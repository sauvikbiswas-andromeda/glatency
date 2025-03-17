package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	totalObjects = 10000
	runs         = 10
)

var batches = []int{1, 100, 500, 1000, 5000}
var contentionRatios = []float64{0.0, 0.01, 0.1, 0.5, 0.9, 0.99}

var metrics = make(map[struct {
	name            string
	batchSize       int
	contentionRatio float64
}][]struct {
	countReadObjects    int
	countWriteObjects   int
	countReadQueries    int
	countWriteQueries   int
	totalTime           time.Duration
	effectiveThroughput float64
})

func main() {
	// Neo4j connection parameters
	uri := "neo4j://localhost:7687"
	username := "neo4j"
	password := "admin@123"

	outputFile := "results.csv"
	reportFile, err := os.Create(outputFile)
	if err != nil {
		log.Fatalf("Failed to create report file: %v", err)
	}
	defer reportFile.Close()

	report := csv.NewWriter(reportFile)
	defer report.Flush()

	// Create a Neo4j driver instance
	driver, err := neo4j.NewDriverWithContext(uri, neo4j.BasicAuth(username, password, ""))
	if err != nil {
		log.Fatalf("Failed to create driver: %v", err)
	}
	defer driver.Close(context.Background())

	// Clear the graph
	if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
		log.Fatalf("Failed to clean up graph: %v", err)
	}

	// Create a constraint on the User, Group nodes and USER_GROUP relationship
	if err := executeTxn(driver, "constraint", 0, 0.0, getConstraintTxn(), true); err != nil {
		log.Fatalf("Failed to create constraint: %v", err)
	}

	// Run the experiments
	RunNodeExperiments(log.New(os.Stdout, "", 0), driver)
	RunEdgeExperiments(log.New(os.Stdout, "", 0), driver)

	// Print the metrics
	header := []string{"Name", "Batch Size", "Contention Ratio", "Total Time", "Effective Throughput"}
	if err := report.Write(header); err != nil {
		log.Fatalf("error writing record to csv: %v", err)
	}
	for key, metric := range metrics {
		totalTimeSlice := []float64{}
		effectiveThroughputSlice := []float64{}
		for _, m := range metric {
			record := []string{
				key.name,
				fmt.Sprintf("%d", key.batchSize),
				fmt.Sprintf("%.2f", key.contentionRatio),
				fmt.Sprintf("%d", m.totalTime.Milliseconds()),
				fmt.Sprintf("%.2f", m.effectiveThroughput),
			}
			if err := report.Write(record); err != nil {
				log.Fatalf("error writing record to csv: %v", err)
			}
			totalTimeSlice = append(totalTimeSlice, float64(m.totalTime.Milliseconds()))
			effectiveThroughputSlice = append(effectiveThroughputSlice, m.effectiveThroughput)
		}
		totalTimeMedian, err := stats.Median(totalTimeSlice)
		if err != nil {
			log.Fatalf("Failed to calculate median of total time: %v", err)
		}
		effectiveWriteThroughputMedian, err := stats.Median(effectiveThroughputSlice)
		if err != nil {
			log.Fatalf("Failed to calculate median of effective write throughput: %v", err)
		}
		log.Printf("%s:\t batch size %d\t contention ratio %.2f\t total time median %.2f\t effective write throughput median %.2f\t",
			key.name, key.batchSize, key.contentionRatio, totalTimeMedian, effectiveWriteThroughputMedian,
		)

	}

	log.Print("Successfully executed all queries.")
}

// latency measures the time taken to execute the queries and can be punched in as defer statement to any function
func latency(name string, batchSize int, contentionRatio float64, start time.Time, countReadObjects, countWriteObjects, countReadQueries, countWriteQueries *int) {
	elapsed := time.Since(start)
	if countReadObjects == nil {
		countReadObjects = new(int)
	}
	if countWriteObjects == nil {
		countWriteObjects = new(int)
	}
	if countReadQueries == nil {
		countReadQueries = new(int)
	}
	if countWriteQueries == nil {
		countWriteQueries = new(int)
	}
	log.Printf("%s: total time %s\t read objects: %d\t write objects: %d\t read queries: %d\t write queries: %d\t effective write throughput: %.2f\t",
		name, elapsed, *countReadObjects, *countWriteObjects, *countReadQueries, *countWriteQueries, float64(*countWriteObjects)/elapsed.Seconds(),
	)
	key := struct {
		name            string
		batchSize       int
		contentionRatio float64
	}{
		name:            name,
		batchSize:       batchSize,
		contentionRatio: contentionRatio,
	}
	if _, ok := metrics[key]; !ok {
		metrics[key] = make([]struct {
			countReadObjects    int
			countWriteObjects   int
			countReadQueries    int
			countWriteQueries   int
			totalTime           time.Duration
			effectiveThroughput float64
		}, 0)
	}
	metrics[key] = append(metrics[key], struct {
		countReadObjects    int
		countWriteObjects   int
		countReadQueries    int
		countWriteQueries   int
		totalTime           time.Duration
		effectiveThroughput float64
	}{
		countReadObjects:    *countReadObjects,
		countWriteObjects:   *countWriteObjects,
		countReadQueries:    *countReadQueries,
		countWriteQueries:   *countWriteQueries,
		totalTime:           elapsed,
		effectiveThroughput: float64(*countReadObjects+*countWriteObjects) / elapsed.Seconds(),
	})
}

func getUserParams(id int) map[string]any {
	return map[string]any{
		"_id":       fmt.Sprintf("user-%d", id),
		"id":        fmt.Sprintf("user-%d", id),
		"email":     fmt.Sprintf("user-%d@gmail.com", id),
		"name":      fmt.Sprintf("User-%d", id),
		"tenantId":  "tenant-X",
		"updatedAt": time.Now().Unix(),
	}
}

func getGroupParams(id int) map[string]any {
	return map[string]any{
		"_id":       fmt.Sprintf("group-%d", id),
		"id":        fmt.Sprintf("group-%d", id),
		"name":      fmt.Sprintf("Group-%d", id),
		"tenantId":  "tenant-X",
		"updatedAt": time.Now().Unix(),
	}
}
