package main

import (
	"fmt"
	"time"

	"raftkv/node"
)

func main() {
	fmt.Println("=== Day 1: Raft leader election (in-process, 3 nodes) ===")

	n1 := node.NewNode(1)
	n2 := node.NewNode(2)
	n3 := node.NewNode(3)
	nodes := []*node.Node{n1, n2, n3}

	for _, n := range nodes {
		n.SetPeers(nodes)
	}

	for _, n := range nodes {
		n.Start()
	}

	// Let an election settle, then report who's leader.
	time.Sleep(1 * time.Second)
	printStatus(nodes)
	
	for i :=0; i<2; i++{
		leader := findLeader(nodes)
		if leader == nil {
			fmt.Println("No leader elected yet — try increasing the sleep above.")
			return
		}

		fmt.Printf("\n>>> Killing current leader: node %d\n\n", leader.ID())
		leader.Stop()

		// Give the remaining nodes time to notice and re-elect.
		time.Sleep(1 * time.Second)
		printStatus(nodes)

	}
	for _, n := range nodes {
		n.Stop()
	}
}

func printStatus(nodes []*node.Node) {
	fmt.Println("--- cluster status ---")
	for _, n := range nodes {
		fmt.Printf("node %d: term=%d state=%s\n", n.ID(), n.Term(), n.State())
	}
	fmt.Println("----------------------")
}

func findLeader(nodes []*node.Node) *node.Node {
	for _, n := range nodes {
		if(!n.IsKilled()){
			if n.State() == node.Leader {
				return n
			}
		}
	}
	return nil
}