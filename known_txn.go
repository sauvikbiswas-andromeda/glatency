package main

import (
	"context"
	"fmt"
	"strings"

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
