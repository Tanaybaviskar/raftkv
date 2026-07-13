package node

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

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

type RequestVoteArgs struct {
	Term        int
	CandidateID int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type LogEntry struct {
	Term    int
	Command string
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type Node struct {
	mu sync.Mutex

	id    int
	peers []*Node

	currentTerm int
	votedFor    int
	state       RaftState

	resetElectionSignal chan struct{}
	stopCh               chan struct{}

	killed   bool
	stopOnce sync.Once

	logEntries  []LogEntry
	commitIndex int

	nextIndex  map[int]int
	matchIndex map[int]int
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

func (n *Node) SetPeers(peers []*Node) {
	n.peers = peers
}

func (n *Node) log(format string, args ...interface{}) {
	prefix := "[node %d term=%d state=%s] "
	fullArgs := append([]interface{}{n.id, n.currentTerm, n.state}, args...)
	log.Printf(prefix+format, fullArgs...)
}

func randomElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (n *Node) Start() {
	go n.run()
}

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

func (n *Node) runElectionTimer() {
	timeout := randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-timer.C:
		n.startElection()
	case <-n.resetElectionSignal:
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

	votes := 1
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
				return
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
			n.nextIndex = make(map[int]int)
			n.matchIndex = make(map[int]int)
			for _, p := range n.peers {
				n.nextIndex[p.id] = len(n.logEntries) + 1
				n.matchIndex[p.id] = 0
			}
		}
		n.mu.Unlock()
	case <-time.After(randomElectionTimeout()):
	case <-n.stopCh:
	}
}

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

// HandleAppendEntries checks log consistency at PrevLogIndex/PrevLogTerm,
// appends new entries, and advances commitIndex. Also serves as heartbeat
// when Entries is empty.
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

	if args.PrevLogIndex > 0 {
		if len(n.logEntries) < args.PrevLogIndex ||
			n.logEntries[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			return AppendEntriesReply{Term: n.currentTerm, Success: false}
		}
	}

	// Find the first point of actual disagreement instead of blindly
	// truncating — if the follower already has these exact entries
	// (same term at same index), leave them alone.
	conflictAt := -1
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + i // 0-based position in n.logEntries
		if idx >= len(n.logEntries) {
			conflictAt = i
			break
		}
		if n.logEntries[idx].Term != e.Term {
			n.logEntries = n.logEntries[:idx] // real conflict: cut everything from here
			conflictAt = i
			break
		}
	}
	if conflictAt != -1 {
		n.logEntries = append(n.logEntries, args.Entries[conflictAt:]...)
	}

	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, len(n.logEntries))
	}

	n.state = Follower
	n.signalReset()
	return AppendEntriesReply{Term: n.currentTerm, Success: true}
}

// runLeader sends AppendEntries (log entries + heartbeat) to every peer
// every tick, and advances its own commitIndex once a majority replicate
// a given entry.
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
		commit := n.commitIndex
		n.mu.Unlock()

		for _, peer := range n.peers {
			if peer.id == n.id {
				continue
			}
			go func(p *Node) {
				n.mu.Lock()
				if n.state != Leader {
					n.mu.Unlock()
					return
				}
				ni := n.nextIndex[p.id]
				prevIdx := ni - 1
				prevTerm := 0
				if prevIdx > 0 {
					prevTerm = n.logEntries[prevIdx-1].Term
				}
				entries := append([]LogEntry{}, n.logEntries[prevIdx:]...)
				n.mu.Unlock()

				reply := p.HandleAppendEntries(AppendEntriesArgs{
					Term:         term,
					LeaderID:     n.id,
					PrevLogIndex: prevIdx,
					PrevLogTerm:  prevTerm,
					Entries:      entries,
					LeaderCommit: commit,
				})

				n.mu.Lock()
				defer n.mu.Unlock()
				if reply.Term > n.currentTerm {
					n.stepDown(reply.Term)
					return
				}
				if n.state != Leader || n.currentTerm != term {
					return
				}
				if reply.Success {
					n.matchIndex[p.id] = prevIdx + len(entries)
					n.nextIndex[p.id] = n.matchIndex[p.id] + 1
					n.advanceCommitIndex()
				} else if n.nextIndex[p.id] > 1 {
					// Log mismatch: back off and retry an earlier index next tick.
					n.nextIndex[p.id]--
				}
			}(peer)
		}

		select {
		case <-ticker.C:
		case <-n.stopCh:
			return
		}
	}
}

// advanceCommitIndex must be called with n.mu held. It sets commitIndex to
// the highest index replicated on a majority of nodes (leader + matchIndex
// values) — the core safety rule: an entry is only "committed" once a
// majority durably have it.
func (n *Node) advanceCommitIndex() {
	for idx := len(n.logEntries); idx > n.commitIndex; idx-- {
		count := 1 // leader has it
		for _, mi := range n.matchIndex {
			if mi >= idx {
				count++
			}
		}
		if count >= len(n.peers)/2+1 && n.logEntries[idx-1].Term == n.currentTerm {
			n.commitIndex = idx
			break
		}
	}
}

func (n *Node) stepDown(newTerm int) {
	n.log("stepping down, saw higher term %d", newTerm)
	n.currentTerm = newTerm
	n.state = Follower
	n.votedFor = -1
	n.signalReset()
}

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

// Propose appends a new command to the leader's log if this node is
// currently the leader. Replication happens on the next heartbeat tick via
// runLeader. Returns an error if called on a non-leader — in a real system
// the caller would use this to redirect the client to the actual leader.
func (n *Node) Propose(command string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return fmt.Errorf("node %d is not the leader (state=%s)", n.id, n.state)
	}
	n.logEntries = append(n.logEntries, LogEntry{Term: n.currentTerm, Command: command})
	return nil
}

func (n *Node) LogEntries() []LogEntry {
	n.mu.Lock()
	defer n.mu.Unlock()
	return append([]LogEntry{}, n.logEntries...)
}

func (n *Node) CommitIndex() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}