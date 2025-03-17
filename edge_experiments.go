package main

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"golang.org/x/sync/errgroup"
)

func RunEdgeExperiments(log *log.Logger, driver neo4j.DriverWithContext) {
	log.Print("running experiments")
	for _, batchSize := range batches {
		log.Printf("batch size: %d", batchSize)
		for _, contentionRatio := range contentionRatios {
			log.Printf("contention ratio: %.2f", contentionRatio)
			for run := 0; run < runs; run++ {
				log.Printf("run: %d", run+1)
				// Create test sets

				// preCreatedObjects := int(contentionRatio * totalObjects)

				queriesCreateUserNodes := make([]ParamedCql, 0)
				queriesCreateGroupNodes := make([]ParamedCql, 0)
				for i := 0; i < int(math.Sqrt(totalObjects)); i++ {
					queriesCreateUserNodes = append(queriesCreateUserNodes, ParamedCql{
						Query:  "CREATE (n:User{`~id`: $_id}) SET n += {email: $email, id: $id, name: $name, tenantId: $tenantId, updatedAt: $updatedAt, isShellEntity: (coalesce(n.isShellEntity, true) AND false), deletedAt: null}",
						Params: getUserParams(i),
					})
					queriesCreateGroupNodes = append(queriesCreateGroupNodes, ParamedCql{
						Query:  "CREATE (n:Group{`~id`: $_id}) SET n += {id: $id, name: $name, tenantId: $tenantId, updatedAt: $updatedAt, isShellEntity: (coalesce(n.isShellEntity, true) AND false), deletedAt: null}",
						Params: getGroupParams(i),
					})
				}

				queriesCreateEdges := make([]ParamedCql, 0)
				for i := 0; i < int(math.Sqrt(totalObjects)); i++ {
					for j := 0; j < int(math.Sqrt(totalObjects)); j++ {
						queriesCreateEdges = append(queriesCreateEdges, ParamedCql{
							Query:  "MATCH (u:User{`~id`: $user_id}), (g:Group{`~id`: $group_id}) CREATE (g)-[r:GROUP_USER_BINDING{`~id`: $user_id + \".\" + $group_id}]->(u)",
							Params: map[string]any{"user_id": fmt.Sprintf("user-%d", i), "group_id": fmt.Sprintf("group-%d", j)},
						})
					}
				}

				random := generateUniqueRandomNumbers(0, int(math.Sqrt(totalObjects)), int(math.Sqrt(totalObjects)*contentionRatio))

				queriesUpdateUserNodes := make([]ParamedCql, 0)
				queriesUpdateGroupNodes := make([]ParamedCql, 0)
				for _, i := range random {
					queriesUpdateUserNodes = append(queriesUpdateUserNodes, ParamedCql{
						Query:  "MATCH (n:User{`~id`: $_id}) SET n += {name: $name + \"-Altered\", updatedAt: $updatedAt}",
						Params: getUserParams(i),
					})
					queriesUpdateGroupNodes = append(queriesUpdateGroupNodes, ParamedCql{
						Query:  "MATCH (n:Group{`~id`: $_id}) SET n += {name: $name + \"-Altered\", updatedAt: $updatedAt}",
						Params: getGroupParams(i),
					})
				}

				// Clear the graph
				if err := executeTxn(driver, "cleanup", 0, 0.0, cleanUpGraphTxn(), true); err != nil {
					log.Fatalf("Failed to clean up graph: %v", err)
				}
				executeBlindCreateInTxnBatches(driver, "user nodes", queriesCreateUserNodes, batchSize, contentionRatio)
				executeBlindCreateInTxnBatches(driver, "group nodes", queriesCreateGroupNodes, batchSize, contentionRatio)

				g := new(errgroup.Group)
				st := time.Now()
				g.Go(func() error {
					return executeBlindCreateInTxnBatches(driver, "edges", queriesCreateEdges, batchSize, contentionRatio)
				})
				g.Go(func() error {
					return executeBlindCreateInTxnBatches(driver, "update user nodes", queriesUpdateUserNodes, batchSize, contentionRatio)
				})
				g.Go(func() error {
					return executeBlindCreateInTxnBatches(driver, "update group nodes", queriesUpdateGroupNodes, batchSize, contentionRatio)
				})
				if err := g.Wait(); err != nil {
					log.Fatalf("Failed to execute transactions: %v", err)
				}
				latency("update operation", batchSize, contentionRatio, st, nil, nil, nil, nil)
			}
		}
	}
}
