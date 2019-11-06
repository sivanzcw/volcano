package state

import (
	"volcano.sh/volcano/pkg/apis/scheduling/v1alpha2"
)

type closingState struct {
	queue *v1alpha2.Queue
}

func (cs *closingState) Execute(action v1alpha2.QueueAction) error {
	switch action {
	case v1alpha2.OpenQueueAction:
		return OpenQueue(cs.queue)
	case v1alpha2.CloseQueueAction:
		return SyncQueue(cs.queue)
	default:
		return SyncQueue(cs.queue)
	}

	return nil
}
