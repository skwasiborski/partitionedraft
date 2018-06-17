package main

import (
	"sync"

	"github.com/skwasiborski/partitionedraft/actor"
	"github.com/skwasiborski/partitionedraft/raft"
)

type Node interface {
	ProcessMessage(actorId string, message interface{}) (interface{}, error)
	RequestVote(actorId string, message raft.VoteRequest) (interface{}, error)
	AppendEntries(actorId string, message raft.AppendEntriesRequest) (interface{}, error)
}

type nodeImpl struct {
	Actors       map[string]*actor.Actor
	ActorFactory func(actorId string) *actor.Actor
	mux          sync.Mutex
}

func (n *nodeImpl) GetActor(actorId string) *actor.Actor {
	n.mux.Lock()
	defer n.mux.Unlock()

	actor, ok := n.Actors[actorId]
	if !ok {
		actor = n.ActorFactory(actorId)
		n.Actors[actorId] = actor
	}

	return actor
}

func (n *nodeImpl) ProcessMessage(actorId string, message interface{}) (interface{}, error) {
	return n.GetActor(actorId).ProcessMessage(message)
}

func (n *nodeImpl) RequestVote(actorId string, message raft.VoteRequest) (interface{}, error) {
	return n.GetActor(actorId).RequestVote(message)
}

func (n *nodeImpl) AppendEntries(actorId string, message raft.AppendEntriesRequest) (interface{}, error) {
	return n.GetActor(actorId).AppendEntries(message)
}
