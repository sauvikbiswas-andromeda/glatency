package main

import (
	"math/rand"
)

func generateUniqueRandomNumbers(start, end, count int) []int {
	// Use a map to track unique numbers
	uniqueNumbers := make(map[int]struct{})
	randomNumbers := []int{}

	// Generate unique random numbers
	for len(randomNumbers) < count {
		num := rand.Intn(end-start) + start   // Generate a random number between start and end
		if _, ok := uniqueNumbers[num]; !ok { // Check if the number is already in the map
			uniqueNumbers[num] = struct{}{}
			randomNumbers = append(randomNumbers, num)
		}
	}

	return randomNumbers
}
