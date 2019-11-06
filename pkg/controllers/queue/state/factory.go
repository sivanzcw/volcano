package state

import (
	"volcano.sh/volcano/pkg/apis/scheduling/v1alpha2"
)

//State interface
type State interface {
	// Execute executes the actions based on current state.
	Execute(action v1alpha2.QueueAction) error
}

//ActionFn will open or close queue.
type ActionFn func(queue *v1alpha2.Queue) error

var (
	// SyncQueue will sync queue status.
	SyncQueue ActionFn
	// OpenQueue will set state of queue to open
	OpenQueue ActionFn
	// CloseQueue will set state of queue to close
	CloseQueue ActionFn
)

//NewState gets the state from queue status
func NewState(queue *v1alpha2.Queue) State {
	switch queue.Status.State {
	case "", v1alpha2.QueueStateOpen:
		return &openState{queue: queue}
	case v1alpha2.QueueStateClosed:
		return &closedState{queue: queue}
	case v1alpha2.QueueStateClosing:
		return &closingState{queue: queue}
	}

	return nil
}
