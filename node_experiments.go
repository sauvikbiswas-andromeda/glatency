package main

import (
	"log"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

func RunNodeExperiments(log *log.Logger, driver neo4j.DriverWithContext) {
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

				ExperimentMerge(driver, queriesCreatePartial, queriesMerge, contentionRatio)
				ExperimentBatchedMerge(driver, queriesCreatePartial, queriesMerge, batchSize, contentionRatio)
				ExperimentBatchedMatchInTxnBatches(driver, queriesCreatePartial, queriesCreate, queriesMerge, batchSize, contentionRatio)
				ExperimentBatchedMatchAndCreate(driver, queriesCreatePartial, queriesCreate, batchSize, preCreatedObjects, contentionRatio)
				ExperimentBatchedMatchAndBatchedMerge(driver, queriesCreatePartial, queriesMerge, batchSize, preCreatedObjects, contentionRatio)
				ExperimentBatchedMatchAndBatchedMergeWithUnwind(driver, queriesCreatePartial, queriesMerge, batchSize, preCreatedObjects, contentionRatio)
				ExperimentBatchedMatchAndBatchedCreate(driver, queriesCreatePartial, queriesCreate, batchSize, preCreatedObjects, contentionRatio)
				ExperimentBatchedMatchAndBatchedCreateWithUnwind(driver, queriesCreatePartial, queriesCreate, batchSize, preCreatedObjects, contentionRatio)
				ExperimentBatchedMatchAndBatchedMergeInTxnBatches(driver, queriesCreatePartial, queriesMerge, batchSize, contentionRatio)
				ExperimentBatchedMatchAndBatchedMergeWithUnwindInTxnBatches(driver, queriesCreatePartial, queriesMerge, batchSize, contentionRatio)
				ExperimentBatchedMatchAndBatchedCreateInTxnBatches(driver, queriesCreatePartial, queriesCreate, batchSize, contentionRatio)
				ExperimentBatchedMatchAndBatchedCreateWithUnwindInTxnBatches(driver, queriesCreatePartial, queriesCreate, batchSize, contentionRatio)
			}
		}
	}
}
