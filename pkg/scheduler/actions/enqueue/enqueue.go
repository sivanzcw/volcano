/*
Copyright 2019 The Kubernetes Authors.

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

package enqueue

import (
	"k8s.io/klog"

	"volcano.sh/volcano/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/util"
)

var (
	// defaultOverUsedResourceRatio defines the default overUsed resource ratio for enqueue action
	defaultOverUsedResourceRatio = 1.2
)

type enqueueAction struct {
	ssn *framework.Session

	arguments framework.Arguments
}

func New(args framework.Arguments) framework.Action {
	return &enqueueAction{
		arguments: args,
	}
}

func (enqueue *enqueueAction) Name() string {
	return "enqueue"
}

func (enqueue *enqueueAction) Initialize() {}

func (enqueue *enqueueAction) Execute(ssn *framework.Session) {
	klog.V(3).Infof("Enter Enqueue ...")
	defer klog.V(3).Infof("Leaving Enqueue ...")

	overUsedResourceRatio := defaultOverUsedResourceRatio
	enqueue.arguments.GetFloat64(&overUsedResourceRatio, conf.OverUsedResourceRatio)

	queues := util.NewPriorityQueue(ssn.QueueOrderFn)
	queueMap := map[api.QueueID]*api.QueueInfo{}

	jobsMap := map[api.QueueID]*util.PriorityQueue{}

	for _, job := range ssn.Jobs {
		if queue, found := ssn.Queues[job.Queue]; !found {
			klog.Errorf("Failed to find Queue <%s> for Job <%s/%s>",
				job.Queue, job.Namespace, job.Name)
			continue
		} else {
			if _, existed := queueMap[queue.UID]; !existed {
				klog.V(3).Infof("Added Queue <%s> for Job <%s/%s>",
					queue.Name, job.Namespace, job.Name)

				queueMap[queue.UID] = queue
				queues.Push(queue)
			}
		}

		if job.PodGroup.Status.Phase == scheduling.PodGroupPending {
			if _, found := jobsMap[job.Queue]; !found {
				jobsMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
			}
			klog.V(3).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
			jobsMap[job.Queue].Push(job)
		}
	}

	klog.V(3).Infof("Try to enqueue PodGroup to %d Queues", len(jobsMap))

	emptyRes := api.EmptyResource()
	nodesIdleRes := api.EmptyResource()
	for _, node := range ssn.Nodes {
		nodesIdleRes.Add(node.Allocatable.Clone().Multi(overUsedResourceRatio).Sub(node.Used))
	}

	for {
		if queues.Empty() {
			break
		}

		if nodesIdleRes.Less(emptyRes) {
			klog.V(3).Infof("Node idle resource is overused, ignore it.")
			break
		}

		queue := queues.Pop().(*api.QueueInfo)

		// Found "high" priority job
		jobs, found := jobsMap[queue.UID]
		if !found || jobs.Empty() {
			continue
		}
		job := jobs.Pop().(*api.JobInfo)

		inqueue := false

		if job.PodGroup.Spec.MinResources == nil {
			inqueue = true
		} else {
			pgResource := api.NewResource(*job.PodGroup.Spec.MinResources)
			if ssn.JobEnqueueable(job) && pgResource.LessEqual(nodesIdleRes) {
				nodesIdleRes.Sub(pgResource)
				inqueue = true
			}
		}

		if inqueue {
			job.PodGroup.Status.Phase = scheduling.PodGroupInqueue
			ssn.Jobs[job.UID] = job
		}

		// Added Queue back until no job in Queue.
		queues.Push(queue)
	}
}

func (enqueue *enqueueAction) UnInitialize() {}
