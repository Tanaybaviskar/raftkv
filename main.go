package main

import (
	"fmt"
	"time"

	"raftkv/node"
)

func main() {
	fmt.Println("=== Day 4: leader-only writes + conflict handling (3 nodes) ===")

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

	time.Sleep(1 * time.Second)
	leader := findLeader(nodes)
	if leader == nil {
		fmt.Println("No leader elected — retry.")
		return
	}
	fmt.Printf("Leader is node %d\n", leader.ID())

	// Normal path: propose on the real leader.
	if err := leader.Propose("set x 1"); err != nil {
		fmt.Println("unexpected error:", err)
	}
	leader.Propose("set y 2")
	leader.Propose("set z 3")

	// Guard test: propose on a follower, should be rejected outright now.
	var follower *node.Node
	for _, n := range nodes {
		if n.ID() != leader.ID() {
			follower = n
			break
		}
	}
	if err := follower.Propose("sneaky write"); err != nil {
		fmt.Printf("Correctly rejected: %v\n", err)
	} else {
		fmt.Println("BUG: follower accepted a write!")
	}

	time.Sleep(500 * time.Millisecond)
	printLogs(nodes)

	for _, n := range nodes {
		n.Stop()
	}
}

func printLogs(nodes []*node.Node) {
	fmt.Println("--- replication status ---")
	for _, n := range nodes {
		fmt.Printf("node %d: commitIndex=%d log=%v\n", n.ID(), n.CommitIndex(), n.LogEntries())
	}
	fmt.Println("---------------------------")
}

func findLeader(nodes []*node.Node) *node.Node {
	for _, n := range nodes {
		if !n.IsKilled() && n.State() == node.Leader {
			return n
		}
	}
	return nil
}