package main

import (
	"fmt"
)

func hashKey(key string, numPartitions int32) int32 {
	// Range partitioning based solely on the first character of the key.
	// We assume the first char is always alphanumeric (0-9, A-Z, a-z), so we
	// map it into a dense range [0,62) and then scale that into the number of
	// partitions.  This ignores any trailing characters entirely.
	if len(key) == 0 {
		return 0
	}
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
		// should not happen, but fall back to 0
		idx = 0
	}
	// scale idx∈[0,62) into [0,numPartitions). multiply before divide
	return (idx * numPartitions) / 62
}

func main() {
	numPartitions := int32(4)

	fmt.Println("Key Distribution Across %d Partitions:", numPartitions)
	fmt.Println("======================================")

	counts := make([]int32, numPartitions)

	// Test digits
	for c := byte('0'); c <= '9'; c++ {
		key := string(c)
		p := hashKey(key, numPartitions)
		fmt.Printf("%s -> Partition %d\n", key, p)
		counts[p]++
	}

	// Test uppercase
	for c := byte('A'); c <= 'Z'; c++ {
		key := string(c)
		p := hashKey(key, numPartitions)
		fmt.Printf("%s -> Partition %d\n", key, p)
		counts[p]++
	}

	// Test lowercase
	for c := byte('a'); c <= 'z'; c++ {
		key := string(c)
		p := hashKey(key, numPartitions)
		fmt.Printf("%s -> Partition %d\n", key, p)
		counts[p]++
	}

	fmt.Println("\nPartition Summary:")
	fmt.Println("==================")
	for i := int32(0); i < numPartitions; i++ {
		fmt.Printf("Partition %d: %d keys\n", i, counts[i])
	}
}
