package queue

import (
	"fmt"
	"reflect"

	schedulingv1alpha2 "volcano.sh/volcano/pkg/apis/scheduling/v1alpha2"
	"volcano.sh/volcano/pkg/controllers/queue/state"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/golang/glog"
)

func (c *Controller) syncQueue(queue *schedulingv1alpha2.Queue, updateStateFn state.UpdateQueueStatusFn) error {
	glog.V(4).Infof("Begin to sync queue %s.", queue.Name)

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

	if updateStateFn != nil {
		updateStateFn(&queueStatus, podGroups)
	} else {
		queueStatus.State = queue.Status.State
	}

	// ignore update when status does not change
	if reflect.DeepEqual(queueStatus, queue.Status) {
		return nil
	}

	newQueue := queue.DeepCopy()
	newQueue.Status = queueStatus
	if _, err := c.vcClient.SchedulingV1alpha2().Queues().UpdateStatus(newQueue); err != nil {
		glog.Errorf("Failed to update status of Queue %s: %v.", newQueue.Name, err)
		return err
	}

	return nil
}

func (c *Controller) openQueue(queue *schedulingv1alpha2.Queue, updateStateFn state.UpdateQueueStatusFn) error {
	glog.V(4).Infof("Begin to open queue %s.", queue.Name)

	newQueue := queue.DeepCopy()
	newQueue.Spec.State = schedulingv1alpha2.QueueStateOpen

	if queue.Spec.State != newQueue.Spec.State {
		if _, err := c.vcClient.SchedulingV1alpha2().Queues().Update(newQueue); err != nil {
			c.recorder.Event(newQueue, v1.EventTypeWarning, string(schedulingv1alpha2.OpenQueueAction),
				fmt.Sprintf("Open queue failed for %v", err))
			return err
		}

		c.recorder.Event(newQueue, v1.EventTypeNormal, string(schedulingv1alpha2.OpenQueueAction),
			fmt.Sprintf("Open queue succeed"))
	} else {
		return nil
	}

	q, err := c.vcClient.SchedulingV1alpha2().Queues().Get(newQueue.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	newQueue = q.DeepCopy()
	if updateStateFn != nil {
		updateStateFn(&newQueue.Status, nil)
	} else {
		return fmt.Errorf("internal error, update state function should be provided")
	}

	if queue.Status.State != newQueue.Status.State {
		if _, err := c.vcClient.SchedulingV1alpha2().Queues().UpdateStatus(newQueue); err != nil {
			c.recorder.Event(newQueue, v1.EventTypeWarning, string(schedulingv1alpha2.OpenQueueAction),
				fmt.Sprintf("Update queue status from %s to %s failed for %v",
					queue.Status.State, newQueue.Status.State, err))
			return err
		}
	}

	return nil
}

func (c *Controller) closeQueue(queue *schedulingv1alpha2.Queue, updateStateFn state.UpdateQueueStatusFn) error {
	glog.V(4).Infof("Begin to close queue %s.", queue.Name)

	newQueue := queue.DeepCopy()
	newQueue.Spec.State = schedulingv1alpha2.QueueStateClosed

	if queue.Spec.State != newQueue.Spec.State {
		if _, err := c.vcClient.SchedulingV1alpha2().Queues().Update(newQueue); err != nil {
			c.recorder.Event(newQueue, v1.EventTypeWarning, string(schedulingv1alpha2.CloseQueueAction),
				fmt.Sprintf("Close queue failed for %v", err))
			return err
		}

		c.recorder.Event(newQueue, v1.EventTypeNormal, string(schedulingv1alpha2.CloseQueueAction),
			fmt.Sprintf("Close queue succeed"))
	} else {
		return nil
	}

	q, err := c.vcClient.SchedulingV1alpha2().Queues().Get(newQueue.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	newQueue = q.DeepCopy()
	podGroups := c.getPodGroups(newQueue.Name)
	if updateStateFn != nil {
		updateStateFn(&newQueue.Status, podGroups)
	} else {
		return fmt.Errorf("internal error, update state function should be provided")
	}

	if queue.Status.State != newQueue.Status.State {
		if _, err := c.vcClient.SchedulingV1alpha2().Queues().UpdateStatus(newQueue); err != nil {
			c.recorder.Event(newQueue, v1.EventTypeWarning, string(schedulingv1alpha2.CloseQueueAction),
				fmt.Sprintf("Update queue status from %s to %s failed for %v",
					queue.Status.State, newQueue.Status.State, err))
			return err
		}
	}

	return nil
}
