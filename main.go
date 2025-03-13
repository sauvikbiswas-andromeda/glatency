package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/montanaflynn/stats"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

const (
	totalObjects = 10000
	runs         = 10
)

var batches = []int{100, 500, 1000, 5000}
var contentionRatios = []float64{0.1, 0.5, 0.9}

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

	// Create a constraint on the User node
	if err := executeTxn(driver, "constraint", 0, 0.0, getConstraintTxn(), true); err != nil {
		log.Fatalf("Failed to create constraint: %v", err)
	}

	log.Print("running experiments")
	for _, batchSize := range batches {
		log.Printf("batch size: %d", batchSize)
		for _, contentionRatio := range contentionRatios {
			log.Printf("contention ratio: %.2f", contentionRatio)
			for run := 0; run < runs; run++ {
				log.Printf("run: %d", run+1)
				// Create test sets

				preCreatedObjects := int(contentionRatio * totalObjects)

				random := generateUniqueRandomNumbers(0, totalObjects, preCreatedObjects)
				queriesCreatePartial := make([]ParamedCql, 0)
				for _, i := range random {
					params := getUserParams(i)
					queriesCreatePartial = append(queriesCreatePartial, ParamedCql{
						Query:  "CREATE (n:User{`~id`: $_id}) SET n += {email: $email, id: $id, name: $name, tenantId: $tenantId, updatedAt: $updatedAt, isShellEntity: (coalesce(n.isShellEntity, true) AND false), deletedAt: null}",
						Params: params,
					})
				}
				queriesMerge := make([]ParamedCql, 0)
				for i := 0; i < totalObjects; i++ {
					params := getUserParams(i)
					queriesMerge = append(queriesMerge, ParamedCql{
						Query:  "MERGE (n:User{`~id`: $_id}) SET n += {email: $email, id: $id, name: $name, tenantId: $tenantId, updatedAt: $updatedAt, isShellEntity: (coalesce(n.isShellEntity, true) AND false), deletedAt: null}",
						Entity: "User",
						Params: params,
					})
				}
				queriesCreate := make([]ParamedCql, 0)
				for i := 0; i < totalObjects; i++ {
					params := getUserParams(i)
					queriesCreate = append(queriesCreate, ParamedCql{
						Query:  "CREATE (n:User{`~id`: $_id}) SET n += {email: $email, id: $id, name: $name, tenantId: $tenantId, updatedAt: $updatedAt, isShellEntity: (coalesce(n.isShellEntity, true) AND false), deletedAt: null}",
						Entity: "User",
						Params: params,
					})
				}

				// Experiment 1: Create nodes using CREATE queries
				// Partially populate the graph
				createTxnFn := getParamedCqlTxn(queriesCreatePartial)
				if err := executeTxn(driver, "create", 1, 1.0, TxnWithMetadata{Txn: createTxnFn, CountWriteObjects: len(queriesCreatePartial), CountWriteQueries: len(queriesCreatePartial)}, false); err != nil {
					log.Fatalf("Failed to execute create queries: %v", err)
				}
				// Execute the merge queries separately
				mergeTxnFn := getParamedCqlTxn(queriesMerge)
				if err := executeTxn(driver, "merge", 1, contentionRatio, TxnWithMetadata{Txn: mergeTxnFn, CountWriteObjects: len(queriesMerge), CountWriteQueries: len(queriesMerge)}, false); err != nil {
					log.Fatalf("Failed to execute merge queries: %v", err)
				}
				// Clear the graph
				if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
					log.Fatalf("Failed to clean up graph: %v", err)
				}

				// Experiment 2: Create nodes using batched CREATE and MERGE queries
				// Partially populate the graph in batches
				if err := executeBlindCreateTxn(driver, "batch create", queriesCreatePartial, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute create queries: %v", err)
				}
				// Execute the merge queries in batches
				if err := executeBlindCreateTxn(driver, "batch merge", queriesMerge, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute create queries: %v", err)
				}
				// Clear the graph
				if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
					log.Fatalf("Failed to clean up graph: %v", err)
				}

				// Experiment 3: Create nodes using txn batched CREATE and MERGE queries
				// Partially populate the graph in batches
				if err := executeBlindCreateTxn(driver, "batch create", queriesCreatePartial, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute create queries: %v", err)
				}
				// Execute the merge queries in batches
				if err := executeBlindCreateInTxnBatches(driver, "txn batch merge", queriesMerge, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute create queries: %v", err)
				}
				// Clear the graph
				if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
					log.Fatalf("Failed to clean up graph: %v", err)
				}

				// Experiment 4: Create nodes using batched CREATE and MATCH + CREATE queries
				// Partially populate the graph in batches
				if err := executeBlindCreateTxn(driver, "batch create", queriesCreatePartial, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute create queries: %v", err)
				}
				// Execute the match + create queries separately
				matchAndCreateTxn := getMatchAndCreateTxn(queriesCreate)
				if err := executeTxn(driver, "match>create", 1, contentionRatio, TxnWithMetadata{Txn: matchAndCreateTxn, CountReadObjects: preCreatedObjects, CountWriteObjects: totalObjects - preCreatedObjects, CountReadQueries: preCreatedObjects, CountWriteQueries: totalObjects - preCreatedObjects}, false); err != nil {
					log.Fatalf("Failed to execute match and create queries: %v", err)
				}
				// Clear the graph
				if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
					log.Fatalf("Failed to clean up graph: %v", err)
				}

				// Experiment 5: Create nodes using batched CREATE and batched MATCH + MERGE queries
				// Partially populate the graph in batches
				if err := executeBlindCreateTxn(driver, "batch create", queriesCreatePartial, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute create queries: %v", err)
				}
				// Execute the conditional merge queries in batches
				if err := executeMatchWithUnwindAndMutateTxn(driver, "batch match+>merge", queriesMerge, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute match and merge batch queries: %v", err)
				}
				// Clear the graph
				if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
					log.Fatalf("Failed to clean up graph: %v", err)
				}

				// Experiment 6: Create nodes using batched CREATE and batched MATCH + CREATE queries
				// Partially populate the graph in batches
				if err := executeBlindCreateTxn(driver, "batch create", queriesCreatePartial, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute create queries: %v", err)
				}
				// Execute the conditional merge queries in batches
				if err := executeMatchWithUnwindAndMutateTxn(driver, "batch match+>create", queriesCreate, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute match and create batch queries: %v", err)
				}
				// Clear the graph
				if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
					log.Fatalf("Failed to clean up graph: %v", err)
				}

				// Experiment 7: Create nodes using txn batched CREATE and batched MATCH + MERGE queries
				// Partially populate the graph in batches
				if err := executeBlindCreateInTxnBatches(driver, "txn batch create", queriesCreatePartial, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute create queries: %v", err)
				}
				// Execute the conditional merge queries in batches
				if err := executeMatchWithUnwindAndMutateInTxnBatches(driver, "txn batch match+>merge", queriesMerge, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute match and merge batch queries: %v", err)
				}
				// Clear the graph
				if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
					log.Fatalf("Failed to clean up graph: %v", err)
				}

				// Experiment 8: Create nodes using txn batched CREATE and batched MATCH + CREATE queries
				// Partially populate the graph in batches
				if err := executeBlindCreateInTxnBatches(driver, "txn batch create", queriesCreatePartial, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute create queries: %v", err)
				}
				// Execute the conditional merge queries in batches
				if err := executeMatchWithUnwindAndMutateInTxnBatches(driver, "txn batch match+>create", queriesCreate, batchSize, contentionRatio); err != nil {
					log.Fatalf("Failed to execute match and create batch queries: %v", err)
				}
				// Clear the graph
				if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
					log.Fatalf("Failed to clean up graph: %v", err)
				}
			}
		}
	}

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

// executeTxn executes a transaction
func executeTxn(driver neo4j.DriverWithContext, name string, batchSize int, contentionRatio float64, txn TxnWithMetadata, suppressMetrics bool) error {
	// Create a session
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	if !suppressMetrics {
		defer latency(name, batchSize, contentionRatio, time.Now(), &txn.CountReadObjects, &txn.CountWriteObjects, &txn.CountReadQueries, &txn.CountWriteQueries)
	}
	defer session.Close(context.Background())

	// Execute the transaction
	_, err := session.ExecuteWrite(context.Background(), txn.Txn)
	return err
}

func getParamedCqlTxn(queries []ParamedCql) neo4j.ManagedTransactionWork {
	return func(tx neo4j.ManagedTransaction) (any, error) {
		for _, query := range queries {
			_, err := tx.Run(context.Background(), query.Query, query.Params)
			if err != nil {
				return nil, fmt.Errorf("failed to execute query '%s': %w", query, err)
			}
		}
		return nil, nil
	}
}

func executeBlindCreateTxn(driver neo4j.DriverWithContext, name string, queries []ParamedCql, batchSize int, contentionRatio float64) error {
	// Create a session
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	countReadObjects, countWriteObjects, countReadQueries, countWriteQueries := 0, 0, 0, 0
	defer latency(name, batchSize, contentionRatio, time.Now(), &countReadObjects, &countWriteObjects, &countReadQueries, &countWriteQueries)
	defer session.Close(context.Background())

	// Execute the transaction
	_, err := session.ExecuteWrite(context.Background(),
		func(tx neo4j.ManagedTransaction) (any, error) {
			for i := 0; i < len(queries); i += batchSize {
				batch := queries[i:min(i+batchSize, len(queries))]
				createQueryParams := make([]map[string]any, 0)
				for _, query := range batch {
					createQueryParams = append(createQueryParams, query.Params)
					countWriteObjects++
				}
				// transform "$" in query to "param."
				newQuery := "UNWIND $params AS param " + strings.ReplaceAll(queries[i].Query, "$", "param.")
				_, err := tx.Run(context.Background(), newQuery, map[string]any{"params": createQueryParams})
				countWriteQueries++
				if err != nil {
					return nil, fmt.Errorf("failed to execute query '%s': %w", newQuery, err)
				}

			}
			return nil, nil
		},
	)
	return err
}

func executeBlindCreateInTxnBatches(driver neo4j.DriverWithContext, name string, queries []ParamedCql, batchSize int, contentionRatio float64) error {
	// Create a session
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	countReadObjects, countWriteObjects, countReadQueries, countWriteQueries := 0, 0, 0, 0
	defer latency(name, batchSize, contentionRatio, time.Now(), &countReadObjects, &countWriteObjects, &countReadQueries, &countWriteQueries)
	defer session.Close(context.Background())

	// Execute the transaction
	for i := 0; i < len(queries); i += batchSize {
		_, err := session.ExecuteWrite(context.Background(),
			func(tx neo4j.ManagedTransaction) (any, error) {
				batch := queries[i:min(i+batchSize, len(queries))]
				createQueryParams := make([]map[string]any, 0)
				for _, query := range batch {
					createQueryParams = append(createQueryParams, query.Params)
					countWriteObjects++
				}
				// transform "$" in query to "param."
				newQuery := "UNWIND $params AS param " + strings.ReplaceAll(queries[i].Query, "$", "param.")
				_, err := tx.Run(context.Background(), newQuery, map[string]any{"params": createQueryParams})
				countWriteQueries++
				if err != nil {
					return nil, fmt.Errorf("failed to execute query '%s': %w", newQuery, err)
				}

				return nil, nil
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func executeMatchAndMutateTxn(driver neo4j.DriverWithContext, name string, queries []ParamedCql, batchSize int, contentionRatio float64) error {
	// Create a session
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	countReadObjects, countWriteObjects, countReadQueries, countWriteQueries := 0, 0, 0, 0
	defer latency(name, batchSize, contentionRatio, time.Now(), &countReadObjects, &countWriteObjects, &countReadQueries, &countWriteQueries)
	defer session.Close(context.Background())

	// Execute the transaction
	_, err := session.ExecuteWrite(context.Background(),
		func(tx neo4j.ManagedTransaction) (any, error) {
			for i := 0; i < len(queries); i += batchSize {
				batch := queries[i:min(i+batchSize, len(queries))]
				searchQuery := fmt.Sprintf("MATCH (n:%s) WHERE n.`~id` IN $_ids RETURN COLLECT(n.`~id`) as entityIds", queries[i].Entity)
				ids := make([]string, 0)
				for _, query := range batch {
					ids = append(ids, query.Params["_id"].(string))
				}
				// fmt.Println(i, searchQuery, map[string]any{"_ids": ids})
				result, err := tx.Run(context.Background(), searchQuery, map[string]any{"_ids": ids})
				countReadQueries++
				if err != nil {
					return nil, fmt.Errorf("failed to execute query '%s': %w", searchQuery, err)
				}
				foundIds := make(map[string]struct{})
				records, err := result.Collect(context.Background())
				if err != nil {
					return nil, fmt.Errorf("failed to collect records: %w", err)
				}
				for _, record := range records {
					foundIdsAny, _ := record.Get("entityIds")
					foundIdSlice := foundIdsAny.([]any)
					for _, id := range foundIdSlice {
						foundIds[id.(string)] = struct{}{}
						countReadObjects++
					}
				}
				// fmt.Println(foundIds)
				createQueryParams := make([]map[string]any, 0)
				for _, query := range batch {
					if _, ok := foundIds[query.Params["_id"].(string)]; !ok {
						createQueryParams = append(createQueryParams, query.Params)
						countWriteObjects++
					}
				}
				// transform "$" in query to "param."
				newQuery := "UNWIND $params AS param " + strings.ReplaceAll(queries[i].Query, "$", "param.")
				_, err = tx.Run(context.Background(), newQuery, map[string]any{"params": createQueryParams})
				countWriteQueries++
				if err != nil {
					return nil, fmt.Errorf("failed to execute query '%s': %w", newQuery, err)
				}

			}
			return nil, nil
		},
	)
	return err
}

func executeMatchWithUnwindAndMutateTxn(driver neo4j.DriverWithContext, name string, queries []ParamedCql, batchSize int, contentionRatio float64) error {
	// Create a session
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	countReadObjects, countWriteObjects, countReadQueries, countWriteQueries := 0, 0, 0, 0
	defer latency(name, batchSize, contentionRatio, time.Now(), &countReadObjects, &countWriteObjects, &countReadQueries, &countWriteQueries)
	defer session.Close(context.Background())

	// Execute the transaction
	_, err := session.ExecuteWrite(context.Background(),
		func(tx neo4j.ManagedTransaction) (any, error) {
			for i := 0; i < len(queries); i += batchSize {
				batch := queries[i:min(i+batchSize, len(queries))]
				searchQuery := fmt.Sprintf("UNWIND $_ids AS _id MATCH (n:%s) WHERE n.`~id` = _id RETURN COLLECT(n.`~id`) AS entityIds", queries[i].Entity)
				ids := make([]string, 0)
				for _, query := range batch {
					ids = append(ids, query.Params["_id"].(string))
				}
				// fmt.Println(i, searchQuery, map[string]any{"_ids": ids})
				result, err := tx.Run(context.Background(), searchQuery, map[string]any{"_ids": ids})
				countReadQueries++
				if err != nil {
					return nil, fmt.Errorf("failed to execute query '%s': %w", searchQuery, err)
				}
				foundIds := make(map[string]struct{})
				records, err := result.Collect(context.Background())
				if err != nil {
					return nil, fmt.Errorf("failed to collect records: %w", err)
				}
				for _, record := range records {
					foundIdsAny, _ := record.Get("entityIds")
					foundIdSlice := foundIdsAny.([]any)
					for _, id := range foundIdSlice {
						foundIds[id.(string)] = struct{}{}
						countReadObjects++
					}
				}
				// fmt.Println(foundIds)
				createQueryParams := make([]map[string]any, 0)
				for _, query := range batch {
					if _, ok := foundIds[query.Params["_id"].(string)]; !ok {
						createQueryParams = append(createQueryParams, query.Params)
						countWriteObjects++
					}
				}
				// transform "$" in query to "param."
				newQuery := "UNWIND $params AS param " + strings.ReplaceAll(queries[i].Query, "$", "param.")
				_, err = tx.Run(context.Background(), newQuery, map[string]any{"params": createQueryParams})
				countWriteQueries++
				if err != nil {
					return nil, fmt.Errorf("failed to execute query '%s': %w", newQuery, err)
				}

			}
			return nil, nil
		},
	)
	return err
}

func executeMatchAndMutateInTxnBatches(driver neo4j.DriverWithContext, name string, queries []ParamedCql, batchSize int, contentionRatio float64) error {
	// Create a session
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	countReadObjects, countWriteObjects, countReadQueries, countWriteQueries := 0, 0, 0, 0
	defer latency(name, batchSize, contentionRatio, time.Now(), &countReadObjects, &countWriteObjects, &countReadQueries, &countWriteQueries)
	defer session.Close(context.Background())

	// Execute the transaction
	for i := 0; i < len(queries); i += batchSize {
		_, err := session.ExecuteWrite(context.Background(),
			func(tx neo4j.ManagedTransaction) (any, error) {
				batch := queries[i:min(i+batchSize, len(queries))]
				searchQuery := fmt.Sprintf("MATCH (n:%s) WHERE n.`~id` IN $_ids RETURN COLLECT(n.`~id`) as entityIds", queries[i].Entity)
				ids := make([]string, 0)
				for _, query := range batch {
					ids = append(ids, query.Params["_id"].(string))
				}
				// fmt.Println(i, searchQuery, map[string]any{"_ids": ids})
				result, err := tx.Run(context.Background(), searchQuery, map[string]any{"_ids": ids})
				countReadQueries++
				if err != nil {
					return nil, fmt.Errorf("failed to execute query '%s': %w", searchQuery, err)
				}
				foundIds := make(map[string]struct{})
				records, err := result.Collect(context.Background())
				if err != nil {
					return nil, fmt.Errorf("failed to collect records: %w", err)
				}
				for _, record := range records {
					foundIdsAny, _ := record.Get("entityIds")
					foundIdSlice := foundIdsAny.([]any)
					for _, id := range foundIdSlice {
						foundIds[id.(string)] = struct{}{}
						countReadObjects++
					}
				}
				// fmt.Println(foundIds)
				createQueryParams := make([]map[string]any, 0)
				for _, query := range batch {
					if _, ok := foundIds[query.Params["_id"].(string)]; !ok {
						createQueryParams = append(createQueryParams, query.Params)
						countWriteObjects++
					}
				}
				// transform "$" in query to "param."
				newQuery := "UNWIND $params AS param " + strings.ReplaceAll(queries[i].Query, "$", "param.")
				_, err = tx.Run(context.Background(), newQuery, map[string]any{"params": createQueryParams})
				countWriteQueries++
				if err != nil {
					return nil, fmt.Errorf("failed to execute query '%s': %w", newQuery, err)
				}

				return nil, nil
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func executeMatchWithUnwindAndMutateInTxnBatches(driver neo4j.DriverWithContext, name string, queries []ParamedCql, batchSize int, contentionRatio float64) error {
	// Create a session
	session := driver.NewSession(context.Background(), neo4j.SessionConfig{
		AccessMode: neo4j.AccessModeWrite,
	})
	countReadObjects, countWriteObjects, countReadQueries, countWriteQueries := 0, 0, 0, 0
	defer latency(name, batchSize, contentionRatio, time.Now(), &countReadObjects, &countWriteObjects, &countReadQueries, &countWriteQueries)
	defer session.Close(context.Background())

	// Execute the transaction
	for i := 0; i < len(queries); i += batchSize {
		_, err := session.ExecuteWrite(context.Background(),
			func(tx neo4j.ManagedTransaction) (any, error) {
				batch := queries[i:min(i+batchSize, len(queries))]
				searchQuery := fmt.Sprintf("UNWIND $_ids AS _id MATCH (n:%s) WHERE n.`~id` = _id RETURN COLLECT(n.`~id`) AS entityIds", queries[i].Entity)
				ids := make([]string, 0)
				for _, query := range batch {
					ids = append(ids, query.Params["_id"].(string))
				}
				// fmt.Println(i, searchQuery, map[string]any{"_ids": ids})
				result, err := tx.Run(context.Background(), searchQuery, map[string]any{"_ids": ids})
				countReadQueries++
				if err != nil {
					return nil, fmt.Errorf("failed to execute query '%s': %w", searchQuery, err)
				}
				foundIds := make(map[string]struct{})
				records, err := result.Collect(context.Background())
				if err != nil {
					return nil, fmt.Errorf("failed to collect records: %w", err)
				}
				for _, record := range records {
					foundIdsAny, _ := record.Get("entityIds")
					foundIdSlice := foundIdsAny.([]any)
					for _, id := range foundIdSlice {
						foundIds[id.(string)] = struct{}{}
						countReadObjects++
					}
				}
				// fmt.Println(foundIds)
				createQueryParams := make([]map[string]any, 0)
				for _, query := range batch {
					if _, ok := foundIds[query.Params["_id"].(string)]; !ok {
						createQueryParams = append(createQueryParams, query.Params)
						countWriteObjects++
					}
				}
				// transform "$" in query to "param."
				newQuery := "UNWIND $params AS param " + strings.ReplaceAll(queries[i].Query, "$", "param.")
				_, err = tx.Run(context.Background(), newQuery, map[string]any{"params": createQueryParams})
				countWriteQueries++
				if err != nil {
					return nil, fmt.Errorf("failed to execute query '%s': %w", newQuery, err)
				}

				return nil, nil
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func getMatchAndCreateTxn(queries []ParamedCql) neo4j.ManagedTransactionWork {
	return func(tx neo4j.ManagedTransaction) (any, error) {
		for _, query := range queries {
			searchQuery := fmt.Sprintf("MATCH (n:%s{`~id`: $_id}) RETURN n LIMIT 1", query.Entity)
			result, err := tx.Run(context.Background(), searchQuery, query.Params)
			if err != nil {
				return nil, fmt.Errorf("failed to execute query '%s': %w", searchQuery, err)
			}
			if !result.Next(context.Background()) {
				_, err := tx.Run(context.Background(), query.Query, query.Params)
				if err != nil {
					return nil, fmt.Errorf("failed to execute query '%s': %w", query, err)
				}
			}
		}
		return nil, nil
	}
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
