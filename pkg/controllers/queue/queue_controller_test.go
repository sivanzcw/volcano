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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	schedulingv1alpha2 "volcano.sh/volcano/pkg/apis/scheduling/v1alpha2"
	vcclient "volcano.sh/volcano/pkg/client/clientset/versioned/fake"
)

func newFakeController() *Controller {
	KubeBatchClientSet := vcclient.NewSimpleClientset()
	KubeClientSet := kubeclient.NewSimpleClientset()

	controller := NewQueueController(KubeClientSet, KubeBatchClientSet)
	return controller
}

func TestAddQueue(t *testing.T) {
	testCases := []struct {
		Name        string
		queue       *schedulingv1alpha2.Queue
		ExpectValue int
	}{
		{
			Name: "AddQueue",
			queue: &schedulingv1alpha2.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c1",
				},
				Spec: schedulingv1alpha2.QueueSpec{
					Weight: 1,
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		c.addQueue(testcase.queue)

		if testcase.ExpectValue != c.queue.Len() {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queue.Len())
		}
	}
}

func TestDeleteQueue(t *testing.T) {
	testCases := []struct {
		Name        string
		queue       *schedulingv1alpha2.Queue
		ExpectValue bool
	}{
		{
			Name: "DeleteQueue",
			queue: &schedulingv1alpha2.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c1",
				},
				Spec: schedulingv1alpha2.QueueSpec{
					Weight: 1,
				},
			},
			ExpectValue: false,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()
		c.podGroups[testcase.queue.Name] = make(map[string]struct{})

		c.deleteQueue(testcase.queue)

		if _, ok := c.podGroups[testcase.queue.Name]; ok != testcase.ExpectValue {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, ok)
		}
	}

}

func TestAddPodGroup(t *testing.T) {
	namespace := "c1"

	testCases := []struct {
		Name        string
		podGroup    *schedulingv1alpha2.PodGroup
		ExpectValue int
	}{
		{
			Name: "addpodgroup",
			podGroup: &schedulingv1alpha2.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha2.PodGroupSpec{
					Queue: "c1",
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		c.addPodGroup(testcase.podGroup)

		if testcase.ExpectValue != c.queue.Len() {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queue.Len())
		}
		if testcase.ExpectValue != len(c.podGroups[testcase.podGroup.Spec.Queue]) {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, len(c.podGroups[testcase.podGroup.Spec.Queue]))
		}
	}

}

func TestDeletePodGroup(t *testing.T) {
	namespace := "c1"

	testCases := []struct {
		Name        string
		podGroup    *schedulingv1alpha2.PodGroup
		ExpectValue bool
	}{
		{
			Name: "deletepodgroup",
			podGroup: &schedulingv1alpha2.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha2.PodGroupSpec{
					Queue: "c1",
				},
			},
			ExpectValue: false,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		key, _ := cache.MetaNamespaceKeyFunc(testcase.podGroup)
		c.podGroups[testcase.podGroup.Spec.Queue] = make(map[string]struct{})
		c.podGroups[testcase.podGroup.Spec.Queue][key] = struct{}{}

		c.deletePodGroup(testcase.podGroup)
		if _, ok := c.podGroups[testcase.podGroup.Spec.Queue][key]; ok != testcase.ExpectValue {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, ok)
		}
	}
}

func TestUpdatePodGroup(t *testing.T) {
	namespace := "c1"

	testCases := []struct {
		Name        string
		podGroupold *schedulingv1alpha2.PodGroup
		podGroupnew *schedulingv1alpha2.PodGroup
		ExpectValue int
	}{
		{
			Name: "updatepodgroup",
			podGroupold: &schedulingv1alpha2.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha2.PodGroupSpec{
					Queue: "c1",
				},
				Status: schedulingv1alpha2.PodGroupStatus{
					Phase: schedulingv1alpha2.PodGroupPending,
				},
			},
			podGroupnew: &schedulingv1alpha2.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha2.PodGroupSpec{
					Queue: "c1",
				},
				Status: schedulingv1alpha2.PodGroupStatus{
					Phase: schedulingv1alpha2.PodGroupRunning,
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		c.updatePodGroup(testcase.podGroupold, testcase.podGroupnew)

		if testcase.ExpectValue != c.queue.Len() {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queue.Len())
		}
	}
}

func TestSyncQueue(t *testing.T) {
	namespace := "c1"

	testCases := []struct {
		Name        string
		podGroup    *schedulingv1alpha2.PodGroup
		queue       *schedulingv1alpha2.Queue
		ExpectValue int32
	}{
		{
			Name: "syncQueue",
			podGroup: &schedulingv1alpha2.PodGroup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pg1",
					Namespace: namespace,
				},
				Spec: schedulingv1alpha2.PodGroupSpec{
					Queue: "c1",
				},
				Status: schedulingv1alpha2.PodGroupStatus{
					Phase: schedulingv1alpha2.PodGroupPending,
				},
			},
			queue: &schedulingv1alpha2.Queue{
				ObjectMeta: metav1.ObjectMeta{
					Name: "c1",
				},
				Spec: schedulingv1alpha2.QueueSpec{
					Weight: 1,
				},
			},
			ExpectValue: 1,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()

		key, _ := cache.MetaNamespaceKeyFunc(testcase.podGroup)
		c.podGroups[testcase.podGroup.Spec.Queue] = make(map[string]struct{})
		c.podGroups[testcase.podGroup.Spec.Queue][key] = struct{}{}

		c.pgInformer.Informer().GetIndexer().Add(testcase.podGroup)
		c.queueInformer.Informer().GetIndexer().Add(testcase.queue)
		c.kbClient.SchedulingV1alpha2().Queues().Create(testcase.queue)

		err := c.syncQueue(testcase.queue)
		item, _ := c.kbClient.SchedulingV1alpha2().Queues().Get(testcase.queue.Name, metav1.GetOptions{})
		if err != nil && testcase.ExpectValue != item.Status.Pending {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queue.Len())
		}
	}

}

func TestProcessNextWorkItem(t *testing.T) {
	testCases := []struct {
		Name        string
		ExpectValue int32
	}{
		{
			Name:        "processNextWorkItem",
			ExpectValue: 0,
		},
	}

	for i, testcase := range testCases {
		c := newFakeController()
		c.queue.Add("test")
		bVal := c.processNextWorkItem()
		fmt.Println("The value of boolean is ", bVal)
		if c.queue.Len() != 0 {
			t.Errorf("case %d (%s): expected: %v, got %v ", i, testcase.Name, testcase.ExpectValue, c.queue.Len())
		}
	}
}
