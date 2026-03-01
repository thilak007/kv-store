package main

import (
	"fmt"
	"hash/fnv"
)

func hashKey(key string, numPartitions int32) int32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int32(h.Sum32()) % numPartitions
}

func main() {
	numPartitions := int32(3)

	fmt.Println("Key Distribution Across 3 Partitions:")
	fmt.Println("======================================")

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		partition := hashKey(key, numPartitions)
		fmt.Printf("%s -> Partition %d (Server %d)\n", key, partition, partition)
	}
}
