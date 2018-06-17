package raft

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/skwasiborski/partitionedraft/mocks"
)

func TestHandleFollowerRequest_GivenMessageOtherThanVoteOrAppend_ThenClosesResponse(t *testing.T) {
	raft := newNotStartedRaft(nil, 5, 0, time.Duration(10000), time.Duration(100000))
	var nonHandledMessagesTests = []request{
		request{0, commandRequest{nil}, make(chan interface{})},
		request{0, readRequest{}, make(chan interface{})},
		request{0, startElectionRequest{}, make(chan interface{})},
		request{0, appendEntriesRequestResult{}, make(chan interface{})},
		request{0, voteRequestResult{}, make(chan interface{})},
		request{0, broadcastRequest{}, make(chan interface{})},
	}

	for _, r := range nonHandledMessagesTests {
		raft.handleFollowerRequest(&r)
		_, open := <-r.response
		if open {
			t.Error("Channel should have been closed")
		}
	}
}

func TestHandleFollowerRequest_GivenVoted_WhenVoteRequest_ThenWillNotVote(t *testing.T) {
	// Arrange
	raft := newNotStartedRaft(nil, 5, 0, time.Duration(10000), time.Duration(100000))
	raft.votedFor = 0

	voteRequest := VoteRequest{}
	req := request{0, voteRequest, make(chan interface{}, 10)}

	// Act
	raft.handleFollowerRequest(&req)

	// Assert
	response := <-req.response
	if response.(VoteResponse).VoteGranted {
		t.Error("Vote granted when already voted for")
	}
}

func TestHandleFollowerRequest_GivenHasLongerLog_WhenVoteRequest_ThenWillNotVote(t *testing.T) {
	// Arrange
	raft := newNotStartedRaft(nil, 5, 0, time.Duration(10000), time.Duration(100000))

	voteRequest := VoteRequest{2, 4, 4, 0}

	arrangeFuncs := []func(r *defaultRaft){
		func(r *defaultRaft) { r.log = []logEntry{logEntry{0, nil}, logEntry{1, nil}} },
		func(r *defaultRaft) {
			r.log = []logEntry{logEntry{0, nil}, logEntry{0, nil}, logEntry{0, nil}, logEntry{0, nil}, logEntry{0, nil}, logEntry{0, nil}}
		},
	}

	for _, r := range arrangeFuncs {
		r(raft)
		req := request{0, voteRequest, make(chan interface{}, 10)}
		// Act
		raft.handleFollowerRequest(&req)
		response := <-req.response

		// Assert
		if response.(VoteResponse).VoteGranted {
			t.Error("Vote granted when log is longer")
		}
	}
}

func TestHandleLeaderRequest(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mocks.NewMockTransport(mockCtrl)
	// raft := New()
}

func newNotStartedRaft(
	transport *Transport,
	clusterSize int,
	nodeID int,
	broadcastTimeout time.Duration,
	electionTimeout time.Duration) *defaultRaft {
	clusterNodes := make([]int, clusterSize)
	for i := 0; i < clusterSize; i++ {
		clusterNodes[i] = i
	}
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

	return result
}
