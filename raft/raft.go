package raft

//go:generate mockgen -destination=../mocks/mock_transport.go -package=mocks github.com/skwasiborski/raft Transport

import (
	"errors"
	"sync/atomic"
	"time"
)

type role int

const (
	follower role = iota
	candidate
	leader
)

const (
	notVoted int = -1
	noLeader int = -1
)

// VoteRequest message requesting a vote
type VoteRequest struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// VoteResponse response to VoteRequest message
type VoteResponse struct {
	NodeID      int
	Term        int
	VoteGranted bool
}

// commandRequest request to append value to log
type commandRequest struct {
	NewValue interface{}
}

type readRequest struct{}

type startElectionRequest struct{}

// AppendEntriesRequest request to append entries to log from leader
type AppendEntriesRequest struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

// AppendEntriesResponse response to AppendEntriesRequest
type AppendEntriesResponse struct {
	NodeID  int
	Term    int
	Success bool
}

type appendEntriesRequestResult struct {
	AppendEntriesRequest
	AppendEntriesResponse
}

type voteRequestResult struct {
	VoteRequest
	VoteResponse
}

type broadcastRequest struct {
}

// Transport interface defining transport for communication between nodes
type Transport interface {
	Send(message interface{}, nodeID int) (interface{}, error)
}

// Raft main implementation of raft
type Raft interface {
	SetCurrentValue(value interface{}) error
	GetCurrentValue() (interface{}, error)
	IsLeader() bool
	GetLeader() int
	RequestVote(message VoteRequest) (interface{}, error)
	AppendEntries(message AppendEntriesRequest) (interface{}, error)
}

type request struct {
	term     int
	message  interface{}
	response chan interface{}
}

type logEntry struct {
	term    int
	message interface{}
}

type defaultRaft struct {
	nodeID                           int
	leaderID                         int
	clusterSize                      int
	broadcastTimeout                 time.Duration
	electionTimeout                  time.Duration
	transport                        *Transport
	c                                chan request
	currentTerm                      int
	votedFor                         int
	commitIndex                      int
	lastApplied                      int
	nextIndex                        map[int]int
	matchIndex                       map[int]int
	log                              []logEntry
	currentRole                      atomic.Value
	receivedHartbeatSinceLastTimeout bool
	pendingResponses                 map[int]chan<- interface{}
	voteReceived                     map[int]bool
	totalVotesReceived               int
	totalOldVotesReceived            int
	clusterNodes                     []int
	// the oldClusterNodes are used to indicate that configuration change is under way and joint consensus should be used
	oldClusterNodes					 []int
}

// New Creates new raft instance and starts main loop
func New(transport *Transport, clusterSize int, nodeID int, broadcastTimeout time.Duration, electionTimeout time.Duration, clusterNodes []int) Raft {
	result := &defaultRaft{
		nodeID,
		noLeader,
		clusterSize,
		broadcastTimeout,
		electionTimeout,
		transport,
		make(chan request),
		0,
		notVoted,
		0,
		0,
		make(map[int]int),
		make(map[int]int),
		[]logEntry{logEntry{0, nil}},
		atomic.Value{},
		false,
		make(map[int]chan<- interface{}),
		make(map[int]bool),
		0,
		0,
		clusterNodes,
		[]int{},
	}

	go result.mainLoop()

	return result
}

func (raft *defaultRaft) mainLoop() {
	electionTimeout := time.NewTicker(raft.electionTimeout).C
	broadcastTimeout := time.NewTicker(raft.broadcastTimeout).C

	for {
		select {
		case request := <-raft.c:
			if request.term < raft.currentTerm {
				// old request just close the channel
				close(request.response)
				break
			}

			if request.term > raft.currentTerm {
				// newer term discovered, reset everything and step down to follower
				raft.currentTerm = request.term
				raft.currentRole.Store(follower)
				raft.votedFor = notVoted
				raft.leaderID = noLeader

				// when steping down close all pending writes. These will not succeed
				for _, responseC := range raft.pendingResponses {
					close(responseC)
				}
			}

			// None of specific handle methods can be called with term newer then current due to fromer if !!!
			// Because of this the case is not handeled in specific handle request methods
			// All responses are also tunneled through main loop so former logic applies
			switch raft.currentRole.Load() {
			case follower:
				raft.handleFollowerRequest(&request)
			case candidate:
				raft.handleCandidateRequest(&request)
			case leader:
				raft.handleLeaderRequest(&request)
			}
		case <-electionTimeout:
			if raft.currentRole.Load() == leader {
				// leader ignore election timeout
				break
			}

			if raft.receivedHartbeatSinceLastTimeout {
				// we got hartbeat since last timeout so no action required
				raft.receivedHartbeatSinceLastTimeout = false
				break
			}

			// change state to candidate
			raft.currentRole.Store(candidate)
			raft.votedFor = notVoted
			raft.totalVotesReceived = 0
			raft.totalOldVotesReceived = 0
			raft.voteReceived = make(map[int]bool)

			go func() { raft.c <- request{raft.currentTerm, startElectionRequest{}, make(chan interface{})} }()
		case <-broadcastTimeout:
			if raft.currentRole.Load() == leader {
				go func() { raft.c <- request{raft.currentTerm, broadcastRequest{}, make(chan interface{})} }()
			}
		}
	}
}

func (raft *defaultRaft) handleFollowerRequest(request *request) {
	defer close(request.response)
	switch message := request.message.(type) {
	case VoteRequest:
		// TODO: should this be here?
		raft.receivedHartbeatSinceLastTimeout = true
		request.response <- raft.handleFollowerVoteRequest(&message)
	case AppendEntriesRequest:
		raft.receivedHartbeatSinceLastTimeout = true
		request.response <- raft.handleFollowerAppendEntriesRequest(&message)
	}
}

func (raft *defaultRaft) handleFollowerAppendEntriesRequest(message *AppendEntriesRequest) AppendEntriesResponse {
	// TODO: handle when log is longer that leaders (with older terms)
	// TODO: handle configuration change message
	if message.PrevLogIndex == len(raft.log)-1 && message.PrevLogTerm == raft.log[len(raft.log)-1].term {
		if message.Entries != nil {
			raft.log = append(raft.log, message.Entries...)
		}

		raft.commitIndex = message.LeaderCommit
		raft.lastApplied = message.LeaderCommit
		raft.leaderID = message.LeaderID

		return AppendEntriesResponse{raft.nodeID, raft.currentTerm, true}
	}

	return AppendEntriesResponse{raft.nodeID, raft.currentTerm, false}
}

func (raft *defaultRaft) handleFollowerVoteRequest(message *VoteRequest) VoteResponse {
	if raft.votedFor != notVoted {
		return VoteResponse{raft.nodeID, raft.currentTerm, false}
	}

	if message.LastLogTerm < raft.log[len(raft.log)-1].term {
		return VoteResponse{raft.nodeID, raft.currentTerm, false}
	}

	if message.LastLogIndex < len(raft.log)-1 {
		return VoteResponse{raft.nodeID, raft.currentTerm, false}
	}

	return VoteResponse{raft.nodeID, raft.currentTerm, true}
}

func (raft *defaultRaft) handleCandidateRequest(r *request) {
	switch message := r.message.(type) {
	case startElectionRequest:
		raft.currentTerm++
		raft.votedFor = raft.nodeID
		raft.voteReceived[raft.nodeID] = true
		raft.totalVotesReceived = 1

		voteRequest := VoteRequest{raft.currentTerm, raft.nodeID, len(raft.log), raft.log[len(raft.log)-1].term}

		for _, nodeID := range raft.clusterNodes {
			if raft.nodeID == nodeID {
				continue
			}

			nodeIDClosure := nodeID
			go func() {
				// TODO: handle errors
				response, _ := (*raft.transport).Send(voteRequest, nodeIDClosure)
				raft.c <- request{voteRequest.Term, voteRequestResult{voteRequest, response.(VoteResponse)}, make(chan interface{})}
			}()
		}
		close(r.response)
	case voteRequestResult:
		if message.VoteGranted {
			if val, ok := raft.voteReceived[message.NodeID]; !ok || !val {
				raft.voteReceived[message.NodeID] = true
				if nodeInConfiguration(raft.clusterNodes, message.NodeID){
					raft.totalVotesReceived++
				}

				if nodeInConfiguration(raft.oldClusterNodes, message.NodeID) {
					raft.totalOldVotesReceived++
				}
			}
		}

		if raft.totalVotesReceived > len(raft.clusterNodes)/2 &&
		   (len(raft.oldClusterNodes) == 0 || raft.totalOldVotesReceived > len(raft.oldClusterNodes)/2) {
			raft.currentRole.Store(leader)
			for _, nodeID := range append(raft.clusterNodes, raft.oldClusterNodes...) {
				raft.nextIndex[nodeID] = len(raft.log)
				raft.matchIndex[nodeID] = 0
				raft.leaderID = raft.nodeID
			}
		}

		close(r.response)
	case VoteRequest:
		r.response <- VoteResponse{raft.nodeID, raft.currentTerm, false}
		close(r.response)
	case AppendEntriesRequest:
		// AppendEntriesRequest with current term means we should step down
		raft.currentRole.Store(follower)
		go func() { raft.c <- *r }()
		// we do not close r.response because the message will be handled again in follower state
	}
}

func (raft *defaultRaft) handleLeaderRequest(r *request) {
	switch message := r.message.(type) {
	case commandRequest:
		// TODO: handle configuration change command:
		//  if external command add Cold+Cnew to log
		//		move clusterNodes to oldClusterNodes, use list from command as clusterNodes
		//  	add special log entry with both Cold and Cnew to log
		//	if internal command i.e. Cold+Cnew commited ad Cnew log entry
	
		raft.log = append(raft.log, logEntry{raft.currentTerm, message.NewValue})
		raft.pendingResponses[len(raft.log)-1] = r.response
		go func() { raft.c <- request{raft.currentTerm, broadcastRequest{}, make(chan interface{})} }()		
		// r.response is not closed here. We ensure that it is closed in appendEntriesRequestResult and while steping down from beeing a leader
	case broadcastRequest:
		for nodeID, nextIndex := range raft.nextIndex {
			if nodeID == raft.nodeID {
				continue
			}

			var appendEntries []logEntry
			if nextIndex < len(raft.log) {
				appendEntries = raft.log[nextIndex:]
			}

			appendEntriesRequest := AppendEntriesRequest{raft.currentTerm, raft.nodeID, nextIndex - 1, raft.log[nextIndex-1].term, appendEntries, raft.commitIndex}

			var nodeIDClosure = nodeID
			go func() {
				// TODO: handle errors
				response, _ := (*raft.transport).Send(appendEntriesRequest, nodeIDClosure)
				raft.c <- request{appendEntriesRequest.Term, appendEntriesRequestResult{appendEntriesRequest, response.(AppendEntriesResponse)}, make(chan interface{})}
			}()
		}
		close(r.response)
	case readRequest:
		r.response <- raft.log[len(raft.log)-1]
		close(r.response)
	case appendEntriesRequestResult:
		if !message.Success {
			raft.nextIndex[message.NodeID]--
			// TODO: immediate retry
			break
		}

		raft.matchIndex[message.NodeID] = message.PrevLogIndex + len(message.Entries)
		raft.nextIndex[message.NodeID] = message.PrevLogIndex + len(message.Entries) + 1

		// TODO: optimize this
		for i := message.PrevLogIndex + 1; i <= message.PrevLogIndex+len(message.Entries); i++ {
			if raft.log[i].term < raft.currentTerm {
				continue
			}

			if raft.commitIndex >= i {
				// i is already committed
				continue
			}

			var replicationNumber = 0
			var replicationNumberOld = 0

			for _, nodeID := range raft.clusterNodes  {
				if raft.matchIndex[nodeID] >= i {
					replicationNumber++
				}
			}

			for _, nodeID := range raft.oldClusterNodes  {
				if raft.matchIndex[nodeID] >= i {
					replicationNumberOld++
				}
			}

			if replicationNumber > raft.clusterSize/2 && 
			   (len(raft.oldClusterNodes) == 0 || replicationNumberOld > len(raft.oldClusterNodes)/2) {
				for commitID := raft.commitIndex + 1; commitID <= i; commitID++ {
					// TODO: handle configuration change command
					//	if commiting from Cold Cnew store only Cnew (i.e. clear oldClusterNodes) and send Cnew Command to all
					//  if commiting Cnew check if should step down i.e. this node is no longer part of cluster
					raft.pendingResponses[commitID] <- raft.log[i].message
					close(raft.pendingResponses[commitID])
					delete(raft.pendingResponses, commitID)
				}

				raft.commitIndex = i
				raft.lastApplied = i
			}
		}
		close(r.response)
	default:
		close(r.response)
	}
}

func (raft *defaultRaft) SetCurrentValue(value interface{}) error {
	responseC := make(chan interface{})
	r := request{raft.currentTerm, commandRequest{value}, responseC}
	raft.c <- r
	response := <-responseC

	// TODO: ensure we always get nil or response. Currently if leader steps down before replicating given log entry the channel will never be closed
	if response == nil {
		return errors.New("error when saving state")
	}

	return nil
}

func (raft *defaultRaft) GetCurrentValue() (interface{}, error) {
	responseC := make(chan interface{})
	r := request{raft.currentTerm, readRequest{}, responseC}
	raft.c <- r
	response := <-responseC

	// nil means the chanel was closed without handling. It can happen if server is in state that does not handle this request
	// or request term is older then current
	if response == nil {
		return nil, errors.New("read failed")
	}

	return response, nil
}

func (raft *defaultRaft) IsLeader() bool {
	return raft.currentRole.Load() == leader
}

func (raft *defaultRaft) GetLeader() int {
	return raft.leaderID
}

func (raft *defaultRaft) RequestVote(message VoteRequest) (interface{}, error) {
	responseC := make(chan interface{})
	r := request{message.Term, message, responseC}
	raft.c <- r
	response := <-responseC

	// nil means the chanel was closed without handling. It can happen if server is in state that does not handle this request
	// or request term is older then current
	if response == nil {
		response = VoteResponse{raft.nodeID, raft.currentTerm, false}
	}

	return response, nil
}

func (raft *defaultRaft) AppendEntries(message AppendEntriesRequest) (interface{}, error) {
	responseC := make(chan interface{})
	r := request{message.Term, message, responseC}
	raft.c <- r
	response := <-responseC

	// nil means the chanel was closed without handling. It can happen if server is in state that does not handle this request
	// or request term is older then current
	if response == nil {
		response = AppendEntriesResponse{raft.nodeID, raft.currentTerm, false}
	}

	return response, nil
}

func nodeInConfiguration(configuration []int, nodeID int) bool {
	for _, i:= range configuration {
		if (i == nodeID){
			return true
		}
	}

	return false
}
