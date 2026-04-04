package main

import (
	"fmt"
)

var numPartitions = int32(3)
var keyspace = "fuzz"

func randomKeyPartition(key string) int32 {
	c := key[0]
	var idx int32
	switch {
	case '0' <= c && c <= '9':
		idx = int32(c - '0') // 0..9
	case 'A' <= c && c <= 'Z':
		idx = int32(c-'A') + 10 // 10..35
	case 'a' <= c && c <= 'z':
		idx = int32(c-'a') + 36 // 36..61
	default:
		idx = 0
	}
	return (idx * numPartitions) / 62
}

func keySuffixPartition(key string) int32 {
	for _, c := range key {
		if c != '0' {
			idx := int32(c - '0')
			return (idx * numPartitions) / 10
		}
	}
	return 0
}

func hashKey(key, keyspace string) int32 {
	if len(key) == 0 {
		return 0
	}
	switch keyspace {
	case "random":
		return randomKeyPartition(key)
	case "fuzz":
		return keySuffixPartition(key[3:]) // Skip "key" prefix to increase variability
	case "ycsb":
		return keySuffixPartition(key[14:]) // Skip "user_usertable" prefix to increase variability
	default:
		return randomKeyPartition(key)
	}
}

func main() {

	fmt.Printf("Key Distribution Across %d Partitions:\n", numPartitions)
	fmt.Println("======================================")

	counts := make([]int32, numPartitions)

	// Test digits
	for c := byte('0'); c <= '9'; c++ {
		key := "key000" + string(c)
		p := hashKey(key, "fuzz")
		fmt.Printf("%s -> Partition %d\n", key, p)
		counts[p]++
	}

	fmt.Println("\nPartition Summary:")
	fmt.Println("==================")
	for i := int32(0); i < numPartitions; i++ {
		fmt.Printf("Partition %d: %d keys\n", i, counts[i])
	}
}
