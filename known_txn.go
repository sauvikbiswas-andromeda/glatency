package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func getConstraintTxn() TxnWithMetadata {
	return TxnWithMetadata{
		Txn: func(tx neo4j.ManagedTransaction) (any, error) {
			nodes := []string{
				"User",
				"Group",
			}
			relationships := []string{
				"GROUP_USER_BINDING",
			}
			for _, node := range nodes {
				_, err := tx.Run(context.Background(), fmt.Sprintf("CREATE CONSTRAINT %s_id IF NOT EXISTS FOR (n:%s) REQUIRE n.`~id` IS UNIQUE", strings.ToLower(node), node), nil)
				if err != nil {
					return nil, fmt.Errorf("failed to create constraint: %w", err)
				}
			}
			for _, relationship := range relationships {
				_, err := tx.Run(context.Background(), fmt.Sprintf("CREATE CONSTRAINT %s_id IF NOT EXISTS FOR ()-[r:%s]-() REQUIRE r.`~id` IS UNIQUE", strings.ToLower(relationship), relationship), nil)
				if err != nil {
					return nil, fmt.Errorf("failed to create constraint: %w", err)
				}
			}
			return nil, nil
		},
	}
}

func cleanUpGraphTxn() TxnWithMetadata {
	return TxnWithMetadata{
		Txn: func(tx neo4j.ManagedTransaction) (any, error) {
			_, err := tx.Run(context.Background(), "MATCH (n) DETACH DELETE n", nil)
			if err != nil {
				return nil, fmt.Errorf("failed to clean up graph: %w", err)
			}
			return nil, nil
		},
	}
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
