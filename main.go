package main

import (
	"fmt"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type Member string

func (m Member) String() string {
	return string(m)
}

func main() {
	members := []consistent.Member{}
	members = append(members, Member("server-1"))
	members = append(members, Member("server-2"))
	members = append(members, Member("server-3"))

	cfg := consistent.Config{
		PartitionCount:    11111, //Select a big PartitionCount if you have too many keys
		ReplicationFactor: 11,    // how many virtual node is created
		Load:              1.25,
		Hasher:            hasher{},
	}

	c := consistent.New(members, cfg)

	datas := make(map[string]string)
	keys := []string{
		"12345",
		"6512142",
		"972352",
		"3333333",
		"123",
		"888888888",
		"11111111",
		"876546454",
		"4352372323",
		"77777777",
		"54124121652121",
		"1",
		"786767",
	}

	for _, val := range keys {
		key := []byte(val)
		owner := c.LocateKey(key)
		datas[val] = owner.String()
		fmt.Printf("%s belongs to %s\n", val, owner.String())
	}

	// Adding new node
	m := Member("server-4")
	c.Add(m)

	var changed int
	for _, val := range keys {
		key := []byte(val)
		owner := c.LocateKey(key)
		if datas[val] != owner.String() {
			changed++
			fmt.Printf("key: %s moved from %s to %s\n", val, datas[val], owner.String())
			datas[val] = owner.String()
		}
	}

	fmt.Printf("\n%d%% of the partitions are relocated\n", (100*changed)/len(keys))

	// removing node
	c.Remove("server-2")
	changed = 0
	for _, val := range keys {
		key := []byte(val)
		owner := c.LocateKey(key)
		if datas[val] != owner.String() {
			changed++
			fmt.Printf("key: %s moved from %s to %s\n", val, datas[val], owner.String())
			datas[val] = owner.String()
		}
	}

	fmt.Printf("\n%d%% of the partitions are relocated\n", (100*changed)/len(keys))

	// adding node
	m = Member("server-5")
	c.Add(m)

	changed = 0
	for _, val := range keys {
		key := []byte(val)
		owner := c.LocateKey(key)
		if datas[val] != owner.String() {
			changed++
			fmt.Printf("key: %s moved from %s to %s\n", val, datas[val], owner.String())
			datas[val] = owner.String()
		}
	}

	fmt.Printf("\n%d%% of the partitions are relocated\n", (100*changed)/len(keys))
}
