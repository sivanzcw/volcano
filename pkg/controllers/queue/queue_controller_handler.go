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

package queue

import (
	"fmt"
	"reflect"

	vcbusv1 "volcano.sh/volcano/pkg/apis/bus/v1alpha1"
	schedulingv1alpha2 "volcano.sh/volcano/pkg/apis/scheduling/v1alpha2"
	"volcano.sh/volcano/pkg/scheduler/api"

	"k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/golang/glog"
)

func (c *Controller) syncQueue(queue *schedulingv1alpha2.Queue) error {
	glog.V(4).Infof("Begin sync queue %s", queue.Name)

	podGroups := c.getPodGroups(queue.Name)
	queueStatus := schedulingv1alpha2.QueueStatus{}

	for _, pgKey := range podGroups {
		// Ignore error here, tt can not occur.
		ns, name, _ := cache.SplitMetaNamespaceKey(pgKey)

		// TODO: check NotFound error and sync local cache.
		pg, err := c.pgLister.PodGroups(ns).Get(name)
		if err != nil {
			return err
		}

		switch pg.Status.Phase {
		case schedulingv1alpha2.PodGroupPending:
			queueStatus.Pending++
		case schedulingv1alpha2.PodGroupRunning:
			queueStatus.Running++
		case schedulingv1alpha2.PodGroupUnknown:
			queueStatus.Unknown++
		case schedulingv1alpha2.PodGroupInqueue:
			queueStatus.Inqueue++
		}
	}

	// If the `state` value is empty, the status of queue will be set as `Open`
	// If the `state` value is `Open`, then the status of queue will also be `Open`
	// If the `state` value is `Closed`, then we need to further consider whether there
	// is a podgroup under the queue. if there is a podgroup under the queue, the status
	// of the queue will be set as `Closing`, while if there is no podgroup under the queue,
	// the status of queue will be set as `Stopped`.
	queueStatus.State = queue.Spec.State
	if len(queueStatus.State) == 0 {
		queueStatus.State = schedulingv1alpha2.QueueStateOpen
	}

	if queueStatus.State == schedulingv1alpha2.QueueStateClosed {
		if len(podGroups) != 0 {
			queueStatus.State = schedulingv1alpha2.QueueStateClosing
		}
	}

	// ignore update when status does not change
	if reflect.DeepEqual(queueStatus, queue.Status) {
		return nil
	}

	newQueue := queue.DeepCopy()
	newQueue.Status = queueStatus

	if _, err := c.kbClient.SchedulingV1alpha2().Queues().UpdateStatus(newQueue); err != nil {
		glog.Errorf("Failed to update status of Queue %s: %v", newQueue.Name, err)
		return err
	}

	return nil
}

func (c *Controller) openQueue(queue *schedulingv1alpha2.Queue) error {
	glog.V(4).Infof("Begin open queue %s", queue.Name)

	queue.Spec.State = schedulingv1alpha2.QueueStateOpen

	if _, err := c.kbClient.SchedulingV1alpha2().Queues().Update(queue); err != nil {
		c.recorder.Event(queue, v1.EventTypeNormal, string(schedulingv1alpha2.OpenQueueAction),
			fmt.Sprintf("Open queue failed for %v", err))
		glog.Errorf("Failed to open queue %s: %v", queue.Name, err)
		return err
	}
	c.recorder.Event(queue, v1.EventTypeNormal, string(schedulingv1alpha2.OpenQueueAction),
		fmt.Sprintf("Open queue succeed"))

	return nil
}

func (c *Controller) closeQueue(queue *schedulingv1alpha2.Queue) error {
	glog.V(4).Infof("Begin close queue %s", queue.Name)

	queue.Spec.State = schedulingv1alpha2.QueueStateClosed

	if _, err := c.kbClient.SchedulingV1alpha2().Queues().Update(queue); err != nil {
		c.recorder.Event(queue, v1.EventTypeNormal, string(schedulingv1alpha2.CloseQueueAction),
			fmt.Sprintf("Close queue failed for %v", err))
		glog.Errorf("Failed to close queue %s: %v", queue.Name, err)
		return err
	}
	c.recorder.Event(queue, v1.EventTypeNormal, string(schedulingv1alpha2.OpenQueueAction),
		fmt.Sprintf("Close queue succeed"))

	return nil
}

func (c *Controller) addQueue(obj interface{}) {
	queue := obj.(*schedulingv1alpha2.Queue)

	req := &api.QueueRequest{
		Name: queue.Name,

		Event:  schedulingv1alpha2.QueueOutOfSyncEvent,
		Action: schedulingv1alpha2.SyncQueueAction,
	}

	c.queue.Add(req)
}

func (c *Controller) deleteQueue(obj interface{}) {
	queue, ok := obj.(*schedulingv1alpha2.Queue)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			glog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		queue, ok = tombstone.Obj.(*schedulingv1alpha2.Queue)
		if !ok {
			glog.Errorf("Tombstone contained object that is not a Queue: %#v", obj)
			return
		}
	}

	c.pgMutex.Lock()
	defer c.pgMutex.Unlock()
	delete(c.podGroups, queue.Name)
}

func (c *Controller) updateQueue(old, new interface{}) {
	oldQueue, ok := old.(*schedulingv1alpha2.Queue)
	if !ok {
		glog.Errorf("Can not covert old object %v to queues.scheduling.sigs.dev", old)
		return
	}

	newQueue, ok := new.(*schedulingv1alpha2.Queue)
	if !ok {
		glog.Errorf("Can not covert new object %v to queues.scheduling.sigs.dev", old)
		return
	}

	if oldQueue.ResourceVersion == newQueue.ResourceVersion {
		return
	}

	c.addQueue(newQueue)

	return
}

func (c *Controller) addPodGroup(obj interface{}) {
	pg := obj.(*schedulingv1alpha2.PodGroup)
	key, _ := cache.MetaNamespaceKeyFunc(obj)

	c.pgMutex.Lock()
	defer c.pgMutex.Unlock()

	if c.podGroups[pg.Spec.Queue] == nil {
		c.podGroups[pg.Spec.Queue] = make(map[string]struct{})
	}
	c.podGroups[pg.Spec.Queue][key] = struct{}{}

	req := &api.QueueRequest{
		Name: pg.Spec.Queue,

		Event:  schedulingv1alpha2.QueueOutOfSyncEvent,
		Action: schedulingv1alpha2.SyncQueueAction,
	}

	c.queue.Add(req)
}

func (c *Controller) updatePodGroup(old, new interface{}) {
	oldPG := old.(*schedulingv1alpha2.PodGroup)
	newPG := new.(*schedulingv1alpha2.PodGroup)

	// Note: we have no use case update PodGroup.Spec.Queue
	// So do not consider it here.
	if oldPG.Status.Phase != newPG.Status.Phase {
		c.addPodGroup(newPG)
	}
}

func (c *Controller) deletePodGroup(obj interface{}) {
	pg, ok := obj.(*schedulingv1alpha2.PodGroup)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			glog.Errorf("Couldn't get object from tombstone %#v", obj)
			return
		}
		pg, ok = tombstone.Obj.(*schedulingv1alpha2.PodGroup)
		if !ok {
			glog.Errorf("Tombstone contained object that is not a PodGroup: %#v", obj)
			return
		}
	}

	key, _ := cache.MetaNamespaceKeyFunc(obj)

	c.pgMutex.Lock()
	defer c.pgMutex.Unlock()

	delete(c.podGroups[pg.Spec.Queue], key)

	req := &api.QueueRequest{
		Name: pg.Spec.Queue,

		Event:  schedulingv1alpha2.QueueOutOfSyncEvent,
		Action: schedulingv1alpha2.SyncQueueAction,
	}

	c.queue.Add(req)
}

func (c *Controller) addCommand(obj interface{}) {
	cmd, ok := obj.(*vcbusv1.Command)
	if !ok {
		glog.Errorf("obj is not Command")
		return
	}

	c.commandQueue.Add(cmd)
}

func (c *Controller) getPodGroups(key string) []string {
	c.pgMutex.RLock()
	defer c.pgMutex.RUnlock()

	if c.podGroups[key] == nil {
		return nil
	}
	podGroups := make([]string, 0, len(c.podGroups[key]))
	for pgKey := range c.podGroups[key] {
		podGroups = append(podGroups, pgKey)
	}

	return podGroups
}
