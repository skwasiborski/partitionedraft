package actor

import (
	"time"

	"github.com/skwasiborski/partitionedraft/raft"
)

type GetStateFunc func() (interface{}, error)
type SetStateFunc func(interface{}) error

type ActorFunc func(message interface{}, getState GetStateFunc, setState SetStateFunc) (interface{}, error)

type actorMessageRequest struct {
	message interface{}
	resultC chan interface{}
	errorC  chan error
}

type Actor struct {
	handler ActorFunc
	raft    raft.Raft
	c       chan actorMessageRequest
}

func New(handler ActorFunc, transport *raft.Transport, clusterSize int, nodeID int, broadcastTimeout time.Duration, clusterNodes []int) *Actor {
	raft := raft.New(transport, clusterSize, nodeID, broadcastTimeout, broadcastTimeout*10, clusterNodes)
	var actor = Actor{handler, raft, make(chan actorMessageRequest)}
	go actor.actorLoop()

	return &actor
}

func (actor *Actor) actorLoop() {
	for {
		request := <-actor.c
		result, err := actor.handler(request.message, actor.getState, actor.setState)
		if err == nil {
			request.errorC <- err
		} else {
			request.resultC <- result
		}
	}
}

func (actor *Actor) getState() (interface{}, error) {
	return actor.raft.GetCurrentValue()
}

func (actor *Actor) setState(value interface{}) error {
	return actor.raft.SetCurrentValue(value)
}

func (actor *Actor) ProcessMessage(message interface{}) (interface{}, error) {
	if actor.raft.IsLeader() {
		var req = actorMessageRequest{message, make(chan interface{}), make(chan error)}

		actor.c <- req
		select {
		case r := <-req.resultC:
			return r, nil
		case err := <-req.errorC:
			return nil, err
		}
	}

	// TODO: forward to leader node
	return nil, nil
}

func (actor *Actor) RequestVote(message raft.VoteRequest) (interface{}, error) {
	return actor.raft.RequestVote(message)
}

func (actor *Actor) AppendEntries(message raft.AppendEntriesRequest) (interface{}, error) {
	return actor.raft.AppendEntries(message)
}
