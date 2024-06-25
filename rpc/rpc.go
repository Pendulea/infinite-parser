package rpc

import (
	manager "pendulev2/set-manager"
	setlib "pendulev2/set2"
)

var Service *RPCService = nil

// Service is the RPC service implementation.
type RPCService struct {
	Sets *setlib.WorkingSets
	SM   *manager.SetManager
}

func Init(sets *setlib.WorkingSets, sm *manager.SetManager) {
	if Service == nil {
		Service = &RPCService{
			Sets: sets,
			SM:   sm,
		}
	}
}
