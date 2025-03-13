package main

import "github.com/neo4j/neo4j-go-driver/v5/neo4j"

type ParamedCql struct {
	Query  string
	Entity string
	Params map[string]any
}

type TxnWithMetadata struct {
	Txn               neo4j.ManagedTransactionWork
	CountReadObjects  int
	CountWriteObjects int
	CountReadQueries  int
	CountWriteQueries int
}
