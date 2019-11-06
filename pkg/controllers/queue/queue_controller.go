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
	"sync"

	"github.com/golang/glog"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	vcbusv1 "volcano.sh/volcano/pkg/apis/bus/v1alpha1"
	schedulingv1alpha2 "volcano.sh/volcano/pkg/apis/scheduling/v1alpha2"
	kbclientset "volcano.sh/volcano/pkg/client/clientset/versioned"
	vcscheme "volcano.sh/volcano/pkg/client/clientset/versioned/scheme"
	infoext "volcano.sh/volcano/pkg/client/informers/externalversions"
	kbinformerfactory "volcano.sh/volcano/pkg/client/informers/externalversions"
	extbusv1 "volcano.sh/volcano/pkg/client/informers/externalversions/bus/v1alpha1"
	kbinformer "volcano.sh/volcano/pkg/client/informers/externalversions/scheduling/v1alpha2"
	lisbusv1 "volcano.sh/volcano/pkg/client/listers/bus/v1alpha1"
	kblister "volcano.sh/volcano/pkg/client/listers/scheduling/v1alpha2"
	"volcano.sh/volcano/pkg/controllers/queue/state"
	"volcano.sh/volcano/pkg/scheduler/api"
)

// Controller manages queue status.
type Controller struct {
	kubeClient kubernetes.Interface
	kbClient   kbclientset.Interface

	// informer
	queueInformer kbinformer.QueueInformer
	pgInformer    kbinformer.PodGroupInformer

	// queueLister
	queueLister kblister.QueueLister
	queueSynced cache.InformerSynced

	// podGroup lister
	pgLister kblister.PodGroupLister
	pgSynced cache.InformerSynced

	cmdInformer extbusv1.CommandInformer
	cmdLister   lisbusv1.CommandLister
	cmdSynced   func() bool

	// queues that need to be updated.
	queue        workqueue.RateLimitingInterface
	commandQueue workqueue.RateLimitingInterface

	pgMutex   sync.RWMutex
	podGroups map[string]map[string]struct{}

	syncHandler        func(req *api.QueueRequest) error
	syncCommandHandler func(cmd *vcbusv1.Command) error

	recorder record.EventRecorder
}

// NewQueueController creates a QueueController
func NewQueueController(
	kubeClient kubernetes.Interface,
	kbClient kbclientset.Interface,
) *Controller {
	factory := kbinformerfactory.NewSharedInformerFactory(kbClient, 0)
	queueInformer := factory.Scheduling().V1alpha2().Queues()
	pgInformer := factory.Scheduling().V1alpha2().PodGroups()

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(vcscheme.Scheme, v1.EventSource{Component: "vc-controller"})

	c := &Controller{
		kubeClient: kubeClient,
		kbClient:   kbClient,

		queueInformer: queueInformer,
		pgInformer:    pgInformer,

		queueLister: queueInformer.Lister(),
		queueSynced: queueInformer.Informer().HasSynced,

		pgLister: pgInformer.Lister(),
		pgSynced: pgInformer.Informer().HasSynced,

		queue:        workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		commandQueue: workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		podGroups:    make(map[string]map[string]struct{}),

		recorder: recorder,
	}

	queueInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addQueue,
		UpdateFunc: c.updateQueue,
		DeleteFunc: c.deleteQueue,
	})

	pgInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addPodGroup,
		UpdateFunc: c.updatePodGroup,
		DeleteFunc: c.deletePodGroup,
	})

	c.cmdInformer = infoext.NewSharedInformerFactory(c.kbClient, 0).Bus().V1alpha1().Commands()
	c.cmdInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			switch obj.(type) {
			case *vcbusv1.Command:
				cmd := obj.(*vcbusv1.Command)
				if cmd.TargetObject.APIVersion == schedulingv1alpha2.SchemeGroupVersion.String() &&
					cmd.TargetObject.Kind == "Queue" {
					return true
				}

				return false
			default:
				return false
			}
		},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: c.addCommand,
		},
	})
	c.cmdLister = c.cmdInformer.Lister()
	c.cmdSynced = c.cmdInformer.Informer().HasSynced

	state.SyncQueue = c.syncQueue
	state.OpenQueue = c.openQueue
	state.CloseQueue = c.closeQueue

	c.syncHandler = c.handleQueue
	c.syncCommandHandler = c.handleCommand

	return c
}

// Run starts QueueController
func (c *Controller) Run(stopCh <-chan struct{}) {
	go c.queueInformer.Informer().Run(stopCh)
	go c.pgInformer.Informer().Run(stopCh)
	go c.cmdInformer.Informer().Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.queueSynced, c.pgSynced, c.cmdSynced) {
		glog.Errorf("unable to sync caches for queue controller")
		return
	}

	go wait.Until(c.worker, 0, stopCh)
	go wait.Until(c.commandWorker, 0, stopCh)
	glog.Infof("QueueController is running ...... ")
}

// worker runs a worker thread that just dequeues items, processes them, and
// marks them done. You may run as many of these in parallel as you wish; the
// workqueue guarantees that they will not end up processing the same `queue`
// at the same time.
func (c *Controller) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(obj)

	req, ok := obj.(*api.QueueRequest)
	if !ok {
		glog.V(2).Infof("%v is not a valid queue request struct", obj)
		return true
	}

	err := c.syncHandler(req)
	if err == nil {
		c.queue.Forget(obj)
		return true
	}

	glog.Errorf("Sync queue %s failed for %v", req.Name, err)

	return true
}

func (c *Controller) handleQueue(req *api.QueueRequest) error {
	queue, err := c.queueLister.Get(req.Name)
	if err != nil {
		return fmt.Errorf("Get queue %s failed for %v", req.Name, err)
	}

	queueState := state.NewState(queue)
	if queueState == nil {
		return fmt.Errorf("Queue %s state %s is invalid.", queue.Name, queue.Status.State)
	}

	if err := queueState.Execute(req.Action); err != nil {
		c.queue.AddRateLimited(req)
		return fmt.Errorf("Sync queue %s failed for %v, event is %v, action is %s.",
			req.Name, err, req.Event, req.Action)
	}

	return nil
}

func (c *Controller) commandWorker() {
	for c.processNextCommand() {
	}
}

func (c *Controller) processNextCommand() bool {
	obj, shutdown := c.commandQueue.Get()
	if shutdown {
		return false
	}
	defer c.commandQueue.Done(obj)

	cmd, ok := obj.(*vcbusv1.Command)
	if !ok {
		glog.V(2).Infof("%v is not a valid Command struct", obj)
		return true
	}

	err := c.syncCommandHandler(cmd)
	if err == nil {
		c.queue.Forget(obj)
		return true
	}

	glog.Errorf("Sync command %s/%s failed for %v", cmd.Namespace, cmd.Name, err)

	return true
}

func (c *Controller) handleCommand(cmd *vcbusv1.Command) error {
	err := c.kbClient.BusV1alpha1().Commands(cmd.Namespace).Delete(cmd.Name, nil)
	if err != nil {
		if true == apierrors.IsNotFound(err) {
			return nil
		}

		c.commandQueue.AddRateLimited(cmd)
		return fmt.Errorf("Failed to delete command <%s/%s> for %v.", cmd.Namespace, cmd.Name, err)
	}

	req := &api.QueueRequest{
		Name:   cmd.TargetObject.Name,
		Event:  schedulingv1alpha2.QueueCommandIssuedEvent,
		Action: schedulingv1alpha2.QueueAction(cmd.Action),
	}

	c.queue.Add(req)

	return nil
}
