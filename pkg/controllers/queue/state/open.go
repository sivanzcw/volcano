package state

import (
	"volcano.sh/volcano/pkg/apis/scheduling/v1alpha2"
)

type openState struct {
	queue *v1alpha2.Queue
}

func (os *openState) Execute(action v1alpha2.QueueAction) error {
	switch action {
	case v1alpha2.OpenQueueAction:
		return SyncQueue(os.queue)
	case v1alpha2.CloseQueueAction:
		return CloseQueue(os.queue)
	default:
		return SyncQueue(os.queue)
	}

	return nil
}
