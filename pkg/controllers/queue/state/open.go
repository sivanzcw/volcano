/*
Copyright 2019 The Volcano Authors.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
		return SyncQueue(os.queue, func(status *v1alpha2.QueueStatus, podGroupList []string) {
			status.State = v1alpha2.QueueStateOpen
			return
		})
	case v1alpha2.CloseQueueAction:
		return CloseQueue(os.queue, func(status *v1alpha2.QueueStatus, podGroupList []string) {
			if len(podGroupList) == 0 {
				status.State = v1alpha2.QueueStateClosed
				return
			}
			status.State = v1alpha2.QueueStateClosing

			return
		})
	default:
		return SyncQueue(os.queue, func(status *v1alpha2.QueueStatus, podGroupList []string) {
			specState := os.queue.Spec.State
			if len(specState) == 0 || specState == v1alpha2.QueueStateOpen {
				status.State = v1alpha2.QueueStateOpen
				return
			}

			if specState == v1alpha2.QueueStateClosed {
				if len(podGroupList) == 0 {
					status.State = v1alpha2.QueueStateClosed
					return
				}
				status.State = v1alpha2.QueueStateClosing

				return
			}

			status.State = v1alpha2.QueueStateUnknown
			return
		})
	}

	return nil
}
