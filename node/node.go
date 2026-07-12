package node

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

// RaftState represents which role a node currently believes it holds.
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RequestVoteArgs / Reply — the RPC used during elections.
type RequestVoteArgs struct {
	Term        int
	CandidateID int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs / Reply — today we only use this as a heartbeat
// (no log entries yet — that's Week 2). A real leader sends this
// periodically; receiving one resets a follower's election timer.
type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Node is a single Raft participant. For Day 1, peers are held as direct
// in-process references so we can test election logic without a network
// layer yet. This gets swapped for a real RPC client in Week 1, Day 5-6,
// without changing the election logic below.
type Node struct {
	mu sync.Mutex

	id    int
	peers []*Node

	currentTerm int
	votedFor    int // -1 means "no vote cast this term"
	state       RaftState

	resetElectionSignal chan struct{}
	stopCh               chan struct{}

	killed   bool
	stopOnce sync.Once
}

func NewNode(id int) *Node {
	return &Node{
		id:                   id,
		votedFor:             -1,
		state:                Follower,
		resetElectionSignal: make(chan struct{}, 1),
		stopCh:               make(chan struct{}),
	}
}

// SetPeers wires up the cluster after all nodes are created.
func (n *Node) SetPeers(peers []*Node) {
	n.peers = peers
}

func (n *Node) log(format string, args ...interface{}) {
	prefix := "[node %d term=%d state=%s] "
	fullArgs := append([]interface{}{n.id, n.currentTerm, n.state}, args...)
	log.Printf(prefix+format, fullArgs...)
}

// randomElectionTimeout returns a randomized duration. Randomization is
// critical in Raft — it's what prevents every follower from becoming a
// candidate at exactly the same moment and splitting votes forever.
func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

// Start launches the node's main loop. Call this once per node after peers
// are wired up.
func (n *Node) Start() {
	go n.run()
}

// Stop is safe to call more than once (e.g. once mid-demo to simulate a
// crash, and once during cleanup) — sync.Once prevents a double-close panic.
func (n *Node) Stop() {
	n.stopOnce.Do(func() {
		n.mu.Lock()
		n.killed = true
		n.mu.Unlock()
		close(n.stopCh)
	})
}

func (n *Node) run() {
	for {
		n.mu.Lock()
		state := n.state
		n.mu.Unlock()

		switch state {
		case Follower, Candidate:
			n.runElectionTimer()
		case Leader:
			n.runLeader()
		}

		select {
		case <-n.stopCh:
			return
		default:
		}
	}
}

// runElectionTimer waits for either an election timeout or a signal that
// resets it (a valid heartbeat or a vote grant came in). If it times out,
// the node starts an election.
func (n *Node) runElectionTimer() {
	timeout := randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		n.startElection()
	case <-n.resetElectionSignal:
		// Heartbeat or vote grant arrived in time — stay a follower.
	case <-n.stopCh:
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.id
	term := n.currentTerm
	n.log("starting election")
	n.mu.Unlock()

	votes := 1 // vote for self
	var votesMu sync.Mutex
	majority := len(n.peers)/2 + 1
	becameLeader := make(chan struct{})
	var once sync.Once

	for _, peer := range n.peers {
		if peer.id == n.id {
			continue
		}
		go func(p *Node) {
			reply := p.HandleRequestVote(RequestVoteArgs{
				Term:        term,
				CandidateID: n.id,
			})

			n.mu.Lock()
			defer n.mu.Unlock()

			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
				return
			}
			if n.state != Candidate || n.currentTerm != term {
				return // stale reply, election already moved on
			}
			if reply.VoteGranted {
				votesMu.Lock()
				votes++
				v := votes
				votesMu.Unlock()
				if v >= majority {
					once.Do(func() { close(becameLeader) })
				}
			}
		}(peer)
	}

	select {
	case <-becameLeader:
		n.mu.Lock()
		if n.state == Candidate && n.currentTerm == term {
			n.state = Leader
			n.log("won election with majority votes")
		}
		n.mu.Unlock()
	case <-time.After(randomElectionTimeout()):
		// Split vote or no majority — loop will retry with a new election.
	case <-n.stopCh:
	}
}

// HandleRequestVote is called (today: directly; later: over RPC) by a
// candidate asking for this node's vote.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.killed {
		return RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}

	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}

	voteGranted := false
	if args.Term == n.currentTerm && (n.votedFor == -1 || n.votedFor == args.CandidateID) {
		n.votedFor = args.CandidateID
		voteGranted = true
		n.signalReset()
		n.log("granted vote to node %d", args.CandidateID)
	}

	return RequestVoteReply{Term: n.currentTerm, VoteGranted: voteGranted}
}

// HandleAppendEntries is today just a heartbeat handler (no log yet).
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.killed {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}

	if args.Term < n.currentTerm {
		return AppendEntriesReply{Term: n.currentTerm, Success: false}
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	n.state = Follower
	n.signalReset()
	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// runLeader periodically sends heartbeats to all peers to assert authority
// and prevent them from starting new elections.
func (n *Node) runLeader() {
	ticker := time.NewTicker(75 * time.Millisecond)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		term := n.currentTerm
		n.mu.Unlock()

		for _, peer := range n.peers {
			if peer.id == n.id {
				continue
			}
			go func(p *Node) {
				reply := p.HandleAppendEntries(AppendEntriesArgs{Term: term, LeaderID: n.id})
				n.mu.Lock()
				if reply.Term > n.currentTerm {
					n.stepDown(reply.Term)
				}
				n.mu.Unlock()
			}(peer)
		}

		select {
		case <-ticker.C:
		case <-n.stopCh:
			return
		}
	}
}

// stepDown must be called with n.mu held. It's how a node reacts to
// discovering a higher term — the core mechanism that lets Raft recover
// after a stale leader or a network partition heals.
func (n *Node) stepDown(newTerm int) {
	n.log("stepping down, saw higher term %d", newTerm)
	n.currentTerm = newTerm
	n.state = Follower
	n.votedFor = -1
	n.signalReset()
}

// signalReset must be called with n.mu held (it only sends on a buffered
// channel, so it won't deadlock).
func (n *Node) signalReset() {
	select {
	case n.resetElectionSignal <- struct{}{}:
	default:
	}
}

func (n *Node) State() RaftState {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) Term() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) ID() int {
	return n.id
}

func (n *Node) IsKilled() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.killed
}