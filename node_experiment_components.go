package main

import (
	"log"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func ExperimentMerge(driver neo4j.DriverWithContext, queriesCreatePartial, queriesMerge []ParamedCql, contentionRatio float64) {
	// Experiment 1: Create nodes using MERGE queries
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
}

func ExperimentBatchedMerge(driver neo4j.DriverWithContext, queriesCreatePartial, queriesMerge []ParamedCql, batchSize int, contentionRatio float64) {
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
}

func ExperimentBatchedMatchInTxnBatches(driver neo4j.DriverWithContext, queriesCreatePartial, queriesCreate, queriesMerge []ParamedCql, batchSize int, contentionRatio float64) {
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
}

func ExperimentBatchedMatchAndCreate(driver neo4j.DriverWithContext, queriesCreatePartial, queriesCreate []ParamedCql, batchSize, preCreatedObjects int, contentionRatio float64) {
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
}

func ExperimentBatchedMatchAndBatchedMerge(driver neo4j.DriverWithContext, queriesCreatePartial, queriesMerge []ParamedCql, batchSize, preCreatedObjects int, contentionRatio float64) {
	// Experiment 5a: Create nodes using batched CREATE and batched MATCH + MERGE queries
	// Partially populate the graph in batches
	if err := executeBlindCreateTxn(driver, "batch create", queriesCreatePartial, batchSize, contentionRatio); err != nil {
		log.Fatalf("Failed to execute create queries: %v", err)
	}
	// Execute the conditional merge queries in batches
	if err := executeMatchAndMutateTxn(driver, "batch match>merge", queriesMerge, batchSize, contentionRatio); err != nil {
		log.Fatalf("Failed to execute match and merge batch queries: %v", err)
	}
	// Clear the graph
	if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
		log.Fatalf("Failed to clean up graph: %v", err)
	}

}

func ExperimentBatchedMatchAndBatchedMergeWithUnwind(driver neo4j.DriverWithContext, queriesCreatePartial, queriesMerge []ParamedCql, batchSize, preCreatedObjects int, contentionRatio float64) {
	// Experiment 5b: Create nodes using batched CREATE and batched MATCH + MERGE queries
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
}

func ExperimentBatchedMatchAndBatchedCreate(driver neo4j.DriverWithContext, queriesCreatePartial, queriesCreate []ParamedCql, batchSize, preCreatedObjects int, contentionRatio float64) {
	// Experiment 6a: Create nodes using batched CREATE and batched MATCH + CREATE queries
	// Partially populate the graph in batches
	if err := executeBlindCreateTxn(driver, "batch create", queriesCreatePartial, batchSize, contentionRatio); err != nil {
		log.Fatalf("Failed to execute create queries: %v", err)
	}
	// Execute the conditional merge queries in batches
	if err := executeMatchAndMutateTxn(driver, "batch match>create", queriesCreate, batchSize, contentionRatio); err != nil {
		log.Fatalf("Failed to execute match and create batch queries: %v", err)
	}
	// Clear the graph
	if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
		log.Fatalf("Failed to clean up graph: %v", err)
	}
}

func ExperimentBatchedMatchAndBatchedCreateWithUnwind(driver neo4j.DriverWithContext, queriesCreatePartial, queriesCreate []ParamedCql, batchSize, preCreatedObjects int, contentionRatio float64) {
	// Experiment 6b: Create nodes using batched CREATE and batched MATCH + CREATE queries
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
}

func ExperimentBatchedMatchAndBatchedMergeInTxnBatches(driver neo4j.DriverWithContext, queriesCreatePartial, queriesMerge []ParamedCql, batchSize int, contentionRatio float64) {
	// Experiment 7a: Create nodes using txn batched CREATE and batched MATCH + MERGE queries
	// Partially populate the graph in batches
	if err := executeBlindCreateInTxnBatches(driver, "txn batch create", queriesCreatePartial, batchSize, contentionRatio); err != nil {
		log.Fatalf("Failed to execute create queries: %v", err)
	}
	// Execute the conditional merge queries in batches
	if err := executeMatchAndMutateInTxnBatches(driver, "txn batch match>merge", queriesMerge, batchSize, contentionRatio); err != nil {
		log.Fatalf("Failed to execute match and merge batch queries: %v", err)
	}
	// Clear the graph
	if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
		log.Fatalf("Failed to clean up graph: %v", err)
	}
}

func ExperimentBatchedMatchAndBatchedMergeWithUnwindInTxnBatches(driver neo4j.DriverWithContext, queriesCreatePartial, queriesMerge []ParamedCql, batchSize int, contentionRatio float64) {
	// Experiment 7b: Create nodes using txn batched CREATE and batched MATCH + MERGE queries
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
}

func ExperimentBatchedMatchAndBatchedCreateInTxnBatches(driver neo4j.DriverWithContext, queriesCreatePartial, queriesCreate []ParamedCql, batchSize int, contentionRatio float64) {
	// Experiment 8a: Create nodes using txn batched CREATE and batched MATCH + CREATE queries
	// Partially populate the graph in batches
	if err := executeBlindCreateInTxnBatches(driver, "txn batch create", queriesCreatePartial, batchSize, contentionRatio); err != nil {
		log.Fatalf("Failed to execute create queries: %v", err)
	}
	// Execute the conditional merge queries in batches
	if err := executeMatchAndMutateInTxnBatches(driver, "txn batch match>create", queriesCreate, batchSize, contentionRatio); err != nil {
		log.Fatalf("Failed to execute match and create batch queries: %v", err)
	}
	// Clear the graph
	if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
		log.Fatalf("Failed to clean up graph: %v", err)
	}
}

func ExperimentBatchedMatchAndBatchedCreateWithUnwindInTxnBatches(driver neo4j.DriverWithContext, queriesCreatePartial, queriesCreate []ParamedCql, batchSize int, contentionRatio float64) {
	// Experiment 8b: Create nodes using txn batched CREATE and batched MATCH + CREATE queries
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
