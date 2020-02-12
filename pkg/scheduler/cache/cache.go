/*
Copyright 2017 The Kubernetes Authors.

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

package cache

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	infov1 "k8s.io/client-go/informers/core/v1"
	schedv1 "k8s.io/client-go/informers/scheduling/v1beta1"
	storagev1 "k8s.io/client-go/informers/storage/v1"
	"k8s.io/client-go/kubernetes"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/volumebinder"

	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/apis/scheduling"
	schedulingscheme "volcano.sh/volcano/pkg/apis/scheduling/scheme"
	"volcano.sh/volcano/pkg/apis/scheduling/v1alpha1"
	"volcano.sh/volcano/pkg/apis/scheduling/v1alpha2"
	vcclient "volcano.sh/volcano/pkg/client/clientset/versioned"
	"volcano.sh/volcano/pkg/client/clientset/versioned/scheme"
	vcinformer "volcano.sh/volcano/pkg/client/informers/externalversions"
	vcinformerv1 "volcano.sh/volcano/pkg/client/informers/externalversions/scheduling/v1alpha1"
	vcinformerv2 "volcano.sh/volcano/pkg/client/informers/externalversions/scheduling/v1alpha2"
	schedulingapi "volcano.sh/volcano/pkg/scheduler/api"
)

func init() {
	schemeBuilder := runtime.SchemeBuilder{
		v1.AddToScheme,
	}

	utilruntime.Must(schemeBuilder.AddToScheme(scheme.Scheme))
}

// New returns a Cache implementation.
func New(config *rest.Config, schedulerName string, defaultQueue string) Cache {
	return newSchedulerCache(config, schedulerName, defaultQueue)
}

//SchedulerCache cache for the kube batch
type SchedulerCache struct {
	sync.Mutex

	kubeclient *kubernetes.Clientset
	vcclient   *vcclient.Clientset

	defaultQueue string
	// schedulerName is the name for kube batch scheduler
	schedulerName string

	podInformer              infov1.PodInformer
	nodeInformer             infov1.NodeInformer
	nsInformer               infov1.NamespaceInformer
	podGroupInformerV1alpha1 vcinformerv1.PodGroupInformer
	podGroupInformerV1alpha2 vcinformerv2.PodGroupInformer
	queueInformerV1alpha1    vcinformerv1.QueueInformer
	queueInformerV1alpha2    vcinformerv2.QueueInformer
	pvInformer               infov1.PersistentVolumeInformer
	pvcInformer              infov1.PersistentVolumeClaimInformer
	scInformer               storagev1.StorageClassInformer
	pcInformer               schedv1.PriorityClassInformer
	quotaInformer            infov1.ResourceQuotaInformer

	Binder        Binder
	Evictor       Evictor
	StatusUpdater StatusUpdater
	VolumeBinder  VolumeBinder

	Recorder record.EventRecorder

	Jobs                 map[schedulingapi.JobID]*schedulingapi.JobInfo
	Nodes                map[string]*schedulingapi.NodeInfo
	Queues               map[schedulingapi.QueueID]*schedulingapi.QueueInfo
	PriorityClasses      map[string]*v1beta1.PriorityClass
	defaultPriorityClass *v1beta1.PriorityClass
	defaultPriority      int32

	NamespaceCollection map[string]*schedulingapi.NamespaceCollection

	errTasks    workqueue.RateLimitingInterface
	deletedJobs workqueue.RateLimitingInterface
}

type defaultBinder struct {
	kubeclient *kubernetes.Clientset
}

//Bind will send bind request to api server
func (db *defaultBinder) Bind(p *v1.Pod, hostname string) error {
	if err := db.kubeclient.CoreV1().Pods(p.Namespace).Bind(&v1.Binding{
		ObjectMeta: metav1.ObjectMeta{Namespace: p.Namespace, Name: p.Name, UID: p.UID, Annotations: p.Annotations},
		Target: v1.ObjectReference{
			Kind: "Node",
			Name: hostname,
		},
	}); err != nil {
		klog.Errorf("Failed to bind pod <%v/%v>: %#v", p.Namespace, p.Name, err)
		return err
	}
	return nil
}

func (db *defaultBinder) Evict(p *v1.Pod) error {
	if err := db.kubeclient.CoreV1().Pods(p.Namespace).Delete(p.Name, nil); err != nil {
		klog.Errorf("Failed to evict pod <%v/%v>: %#v", p.Namespace, p.Name, err)
		return err
	}

	return nil
}

type defaultEvictor struct {
	kubeclient *kubernetes.Clientset
}

//Evict will send delete pod request to api server
func (de *defaultEvictor) Evict(p *v1.Pod) error {
	klog.V(3).Infof("Evicting pod %v/%v", p.Namespace, p.Name)

	if err := de.kubeclient.CoreV1().Pods(p.Namespace).Delete(p.Name, nil); err != nil {
		klog.Errorf("Failed to evict pod <%v/%v>: %#v", p.Namespace, p.Name, err)
		return err
	}
	return nil
}

// defaultStatusUpdater is the default implementation of the StatusUpdater interface
type defaultStatusUpdater struct {
	kubeclient *kubernetes.Clientset
	vcclient   *vcclient.Clientset
}

// following the same logic as podutil.UpdatePodCondition
func podConditionHaveUpdate(status *v1.PodStatus, condition *v1.PodCondition) bool {
	lastTransitionTime := metav1.Now()
	// Try to find this pod condition.
	_, oldCondition := podutil.GetPodCondition(status, condition.Type)

	if oldCondition == nil {
		// We are adding new pod condition.
		return true
	}
	// We are updating an existing condition, so we need to check if it has changed.
	if condition.Status == oldCondition.Status {
		lastTransitionTime = oldCondition.LastTransitionTime
	}

	isEqual := condition.Status == oldCondition.Status &&
		condition.Reason == oldCondition.Reason &&
		condition.Message == oldCondition.Message &&
		condition.LastProbeTime.Equal(&oldCondition.LastProbeTime) &&
		lastTransitionTime.Equal(&oldCondition.LastTransitionTime)

	// Return true if one of the fields have changed.
	return !isEqual
}

// UpdatePodCondition will Update pod with podCondition
func (su *defaultStatusUpdater) UpdatePodCondition(pod *v1.Pod, condition *v1.PodCondition) (*v1.Pod, error) {
	klog.V(3).Infof("Updating pod condition for %s/%s to (%s==%s)", pod.Namespace, pod.Name, condition.Type, condition.Status)
	if podutil.UpdatePodCondition(&pod.Status, condition) {
		return su.kubeclient.CoreV1().Pods(pod.Namespace).UpdateStatus(pod)
	}
	return pod, nil
}

// UpdatePodGroup will Update pod with podCondition
func (su *defaultStatusUpdater) UpdatePodGroup(pg *schedulingapi.PodGroup) (*schedulingapi.PodGroup, error) {
	if pg.Version == schedulingapi.PodGroupVersionV1Alpha1 {
		podgroup := &v1alpha1.PodGroup{}
		if err := schedulingscheme.Scheme.Convert(&pg.PodGroup, podgroup, nil); err != nil {
			klog.Errorf("Error while converting PodGroup to v1alpha1.PodGroup with error: %v", err)
			return nil, err
		}

		updated, err := su.vcclient.SchedulingV1alpha1().PodGroups(podgroup.Namespace).Update(podgroup)
		if err != nil {
			klog.Errorf("Error while updating PodGroup with error: %v", err)
			return nil, err
		}

		podGroupInfo := &schedulingapi.PodGroup{Version: schedulingapi.PodGroupVersionV1Alpha1}
		if err := schedulingscheme.Scheme.Convert(updated, &podGroupInfo.PodGroup, nil); err != nil {
			klog.Errorf("Error while converting v1alpha.PodGroup to api.PodGroup with error: %v", err)
			return nil, err
		}

		return podGroupInfo, nil
	}

	if pg.Version == schedulingapi.PodGroupVersionV1Alpha2 {
		podgroup := &v1alpha2.PodGroup{}
		if err := schedulingscheme.Scheme.Convert(&pg.PodGroup, podgroup, nil); err != nil {
			klog.Errorf("Error while converting PodGroup to v1alpha2.PodGroup with error: %v", err)
			return nil, err
		}

		updated, err := su.vcclient.SchedulingV1alpha2().PodGroups(podgroup.Namespace).Update(podgroup)
		if err != nil {
			klog.Errorf("Error while updating PodGroup with error: %v", err)
		}

		podGroupInfo := &schedulingapi.PodGroup{Version: schedulingapi.PodGroupVersionV1Alpha2}
		if err := schedulingscheme.Scheme.Convert(updated, &podGroupInfo.PodGroup, nil); err != nil {
			klog.Errorf("Error While converting v2alpha.PodGroup to api.PodGroup with error: %v", err)
			return nil, err
		}

		return podGroupInfo, nil
	}

	return nil, fmt.Errorf("invalid PodGroup version: %s", pg.Version)
}

type defaultVolumeBinder struct {
	volumeBinder *volumebinder.VolumeBinder
}

// AllocateVolumes allocates volume on the host to the task
func (dvb *defaultVolumeBinder) AllocateVolumes(task *schedulingapi.TaskInfo, hostname string) error {
	allBound, err := dvb.volumeBinder.Binder.AssumePodVolumes(task.Pod, hostname)
	task.VolumeReady = allBound

	return err
}

// BindVolumes binds volumes to the task
func (dvb *defaultVolumeBinder) BindVolumes(task *schedulingapi.TaskInfo) error {
	// If task's volumes are ready, did not bind them again.
	if task.VolumeReady {
		return nil
	}

	return dvb.volumeBinder.Binder.BindPodVolumes(task.Pod)
}

func newSchedulerCache(config *rest.Config, schedulerName string, defaultQueue string) *SchedulerCache {
	kubeClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(fmt.Sprintf("failed init kubeClient, with err: %v", err))
	}
	vcClient, err := vcclient.NewForConfig(config)
	if err != nil {
		panic(fmt.Sprintf("failed init vcClient, with err: %v", err))
	}
	eventClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(fmt.Sprintf("failed init eventClient, with err: %v", err))
	}

	// create default queue
	defaultQue := v1alpha2.Queue{
		ObjectMeta: metav1.ObjectMeta{
			Name: defaultQueue,
		},
		Spec: v1alpha2.QueueSpec{
			Weight: 1,
		},
	}
	if _, err := vcClient.SchedulingV1alpha2().Queues().Create(&defaultQue); err != nil && !apierrors.IsAlreadyExists(err) {
		panic(fmt.Sprintf("failed init default queue, with err: %v", err))
	}

	sc := &SchedulerCache{
		Jobs:            make(map[schedulingapi.JobID]*schedulingapi.JobInfo),
		Nodes:           make(map[string]*schedulingapi.NodeInfo),
		Queues:          make(map[schedulingapi.QueueID]*schedulingapi.QueueInfo),
		PriorityClasses: make(map[string]*v1beta1.PriorityClass),
		errTasks:        workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		deletedJobs:     workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		kubeclient:      kubeClient,
		vcclient:        vcClient,
		defaultQueue:    defaultQueue,
		schedulerName:   schedulerName,

		NamespaceCollection: make(map[string]*schedulingapi.NamespaceCollection),
	}

	// Prepare event clients.
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: eventClient.CoreV1().Events("")})
	sc.Recorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: schedulerName})

	sc.Binder = &defaultBinder{
		kubeclient: sc.kubeclient,
	}

	sc.Evictor = &defaultEvictor{
		kubeclient: sc.kubeclient,
	}

	sc.StatusUpdater = &defaultStatusUpdater{
		kubeclient: sc.kubeclient,
		vcclient:   sc.vcclient,
	}

	informerFactory := informers.NewSharedInformerFactory(sc.kubeclient, 0)

	sc.pvcInformer = informerFactory.Core().V1().PersistentVolumeClaims()
	sc.pvInformer = informerFactory.Core().V1().PersistentVolumes()
	sc.scInformer = informerFactory.Storage().V1().StorageClasses()
	sc.VolumeBinder = &defaultVolumeBinder{
		volumeBinder: volumebinder.NewVolumeBinder(
			sc.kubeclient,
			sc.nodeInformer,
			sc.pvcInformer,
			sc.pvInformer,
			sc.scInformer,
			30*time.Second,
		),
	}

	// create informer for node information
	sc.nodeInformer = informerFactory.Core().V1().Nodes()
	sc.nodeInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    sc.AddNode,
			UpdateFunc: sc.UpdateNode,
			DeleteFunc: sc.DeleteNode,
		},
		0,
	)

	// create informer for pod information
	sc.podInformer = informerFactory.Core().V1().Pods()
	sc.podInformer.Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch obj.(type) {
				case *v1.Pod:
					pod := obj.(*v1.Pod)
					if !responsibleForPod(pod, schedulerName) {
						if len(pod.Spec.NodeName) == 0 {
							return false
						}
					}
					return true
				default:
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sc.AddPod,
				UpdateFunc: sc.UpdatePod,
				DeleteFunc: sc.DeletePod,
			},
		})

	sc.pcInformer = informerFactory.Scheduling().V1beta1().PriorityClasses()
	sc.pcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.AddPriorityClass,
		UpdateFunc: sc.UpdatePriorityClass,
		DeleteFunc: sc.DeletePriorityClass,
	})

	sc.quotaInformer = informerFactory.Core().V1().ResourceQuotas()
	sc.quotaInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.AddResourceQuota,
		UpdateFunc: sc.UpdateResourceQuota,
		DeleteFunc: sc.DeleteResourceQuota,
	})

	vcinformers := vcinformer.NewSharedInformerFactory(sc.vcclient, 0)
	// create informer for PodGroup(v1alpha1) information
	sc.podGroupInformerV1alpha1 = vcinformers.Scheduling().V1alpha1().PodGroups()
	sc.podGroupInformerV1alpha1.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.AddPodGroupV1alpha1,
		UpdateFunc: sc.UpdatePodGroupV1alpha1,
		DeleteFunc: sc.DeletePodGroupV1alpha1,
	})

	// create informer for PodGroup(v1alpha2) information
	sc.podGroupInformerV1alpha2 = vcinformers.Scheduling().V1alpha2().PodGroups()
	sc.podGroupInformerV1alpha2.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.AddPodGroupV1alpha2,
		UpdateFunc: sc.UpdatePodGroupV1alpha2,
		DeleteFunc: sc.DeletePodGroupV1alpha2,
	})

	// create informer(v1alpha1) for Queue information
	sc.queueInformerV1alpha1 = vcinformers.Scheduling().V1alpha1().Queues()
	sc.queueInformerV1alpha1.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.AddQueueV1alpha1,
		UpdateFunc: sc.UpdateQueueV1alpha1,
		DeleteFunc: sc.DeleteQueueV1alpha1,
	})

	// create informer(v1alpha2) for Queue information
	sc.queueInformerV1alpha2 = vcinformers.Scheduling().V1alpha2().Queues()
	sc.queueInformerV1alpha2.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.AddQueueV1alpha2,
		UpdateFunc: sc.UpdateQueueV1alpha2,
		DeleteFunc: sc.DeleteQueueV1alpha2,
	})

	return sc
}

// Run  starts the schedulerCache
func (sc *SchedulerCache) Run(stopCh <-chan struct{}) {
	go sc.podInformer.Informer().Run(stopCh)
	go sc.nodeInformer.Informer().Run(stopCh)
	go sc.podGroupInformerV1alpha1.Informer().Run(stopCh)
	go sc.podGroupInformerV1alpha2.Informer().Run(stopCh)
	go sc.pvInformer.Informer().Run(stopCh)
	go sc.pvcInformer.Informer().Run(stopCh)
	go sc.scInformer.Informer().Run(stopCh)
	go sc.queueInformerV1alpha1.Informer().Run(stopCh)
	go sc.queueInformerV1alpha2.Informer().Run(stopCh)
	go sc.quotaInformer.Informer().Run(stopCh)

	if options.ServerOpts.EnablePriorityClass {
		go sc.pcInformer.Informer().Run(stopCh)
	}

	// Re-sync error tasks.
	go wait.Until(sc.processResyncTask, 0, stopCh)

	// Cleanup jobs.
	go wait.Until(sc.processCleanupJob, 0, stopCh)
}

// WaitForCacheSync sync the cache with the api server
func (sc *SchedulerCache) WaitForCacheSync(stopCh <-chan struct{}) bool {

	return cache.WaitForCacheSync(stopCh,
		func() []cache.InformerSynced {
			informerSynced := []cache.InformerSynced{
				sc.podInformer.Informer().HasSynced,
				sc.podGroupInformerV1alpha1.Informer().HasSynced,
				sc.podGroupInformerV1alpha2.Informer().HasSynced,
				sc.nodeInformer.Informer().HasSynced,
				sc.pvInformer.Informer().HasSynced,
				sc.pvcInformer.Informer().HasSynced,
				sc.scInformer.Informer().HasSynced,
				sc.queueInformerV1alpha1.Informer().HasSynced,
				sc.queueInformerV1alpha2.Informer().HasSynced,
				sc.quotaInformer.Informer().HasSynced,
			}
			if options.ServerOpts.EnablePriorityClass {
				informerSynced = append(informerSynced, sc.pcInformer.Informer().HasSynced)
			}
			return informerSynced
		}()...,
	)
}

// findJobAndTask returns job and the task info
func (sc *SchedulerCache) findJobAndTask(taskInfo *schedulingapi.TaskInfo) (*schedulingapi.JobInfo, *schedulingapi.TaskInfo, error) {
	job, found := sc.Jobs[taskInfo.Job]
	if !found {
		return nil, nil, fmt.Errorf("failed to find Job %v for Task %v",
			taskInfo.Job, taskInfo.UID)
	}

	task, found := job.Tasks[taskInfo.UID]
	if !found {
		return nil, nil, fmt.Errorf("failed to find task in status %v by id %v",
			taskInfo.Status, taskInfo.UID)
	}

	return job, task, nil
}

// Evict will evict the pod
func (sc *SchedulerCache) Evict(taskInfo *schedulingapi.TaskInfo, reason string) error {
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	job, task, err := sc.findJobAndTask(taskInfo)

	if err != nil {
		return err
	}

	node, found := sc.Nodes[task.NodeName]
	if !found {
		return fmt.Errorf("failed to bind Task %v to host %v, host does not exist",
			task.UID, task.NodeName)
	}

	err = job.UpdateTaskStatus(task, schedulingapi.Releasing)
	if err != nil {
		return err
	}

	// Add new task to node.
	if err := node.UpdateTask(task); err != nil {
		return err
	}

	p := task.Pod

	go func() {
		err := sc.Evictor.Evict(p)
		if err != nil {
			sc.resyncTask(task)
		}
	}()

	if job.PodGroup.Version == schedulingapi.PodGroupVersionV1Alpha1 {
		podgroup := &v1alpha1.PodGroup{}
		if err := schedulingscheme.Scheme.Convert(&job.PodGroup.PodGroup, podgroup, nil); err != nil {
			klog.Errorf("Error while converting PodGroup to v1alpha1.PodGroup with error: %v", err)
			return err
		}
		sc.Recorder.Eventf(podgroup, v1.EventTypeNormal, "Evict", reason)
		return nil
	}

	if job.PodGroup.Version == schedulingapi.PodGroupVersionV1Alpha2 {
		podgroup := &v1alpha2.PodGroup{}
		if err := schedulingscheme.Scheme.Convert(&job.PodGroup.PodGroup, podgroup, nil); err != nil {
			klog.Errorf("Error while converting PodGroup to v1alpha2.PodGroup with error: %v", err)
			return err
		}
		sc.Recorder.Eventf(podgroup, v1.EventTypeNormal, "Evict", reason)
		return nil
	}

	return fmt.Errorf("Invalid PodGroup Version: %s", job.PodGroup.Version)
}

// Bind binds task to the target host.
func (sc *SchedulerCache) Bind(taskInfo *schedulingapi.TaskInfo, hostname string) error {
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	job, task, err := sc.findJobAndTask(taskInfo)

	if err != nil {
		return err
	}

	node, found := sc.Nodes[hostname]
	if !found {
		return fmt.Errorf("failed to bind Task %v to host %v, host does not exist",
			task.UID, hostname)
	}

	err = job.UpdateTaskStatus(task, schedulingapi.Binding)
	if err != nil {
		return err
	}

	// Set `.nodeName` to the hostname
	task.NodeName = hostname

	// Add task to the node.
	if err := node.AddTask(task); err != nil {
		return err
	}

	p := task.Pod

	var pgCopy schedulingapi.PodGroup
	if job.PodGroup != nil {
		pgCopy = schedulingapi.PodGroup{
			Version:  job.PodGroup.Version,
			PodGroup: *job.PodGroup.PodGroup.DeepCopy(),
		}
	}

	go func() {
		if err := sc.Binder.Bind(p, hostname); err != nil {
			sc.resyncTask(task)
		} else {
			klog.Infof("+++++++++++bind <%s/%s> success", p.Namespace, p.Name)
			if err := sc.Binder.Evict(p); err != nil {
				klog.Infof("++++++++evict failed for %v", err)
			} else {
				klog.Infof("+++++++++Evict <%s/%s> success", p.Namespace, p.Name)
			}
			sc.Recorder.Eventf(p, v1.EventTypeNormal, "Scheduled", "Successfully assigned %v/%v to %v", p.Namespace, p.Name, hostname)

			if job.PodGroup != nil {
				msg := fmt.Sprintf("%v/%v tasks in gang unschedulable: %v, %v minAvailable, %v Pending",
					len(job.TaskStatusIndex[schedulingapi.Pending]),
					len(job.Tasks),
					scheduling.PodGroupReady,
					job.MinAvailable,
					len(job.TaskStatusIndex[schedulingapi.Pending]))
				sc.recordPodGroupEvent(&pgCopy, v1.EventTypeNormal, string(scheduling.PodGroupScheduled), msg)
			}
		}
	}()

	return nil
}

// AllocateVolumes allocates volume on the host to the task
func (sc *SchedulerCache) AllocateVolumes(task *schedulingapi.TaskInfo, hostname string) error {
	return sc.VolumeBinder.AllocateVolumes(task, hostname)
}

// BindVolumes binds volumes to the task
func (sc *SchedulerCache) BindVolumes(task *schedulingapi.TaskInfo) error {
	return sc.VolumeBinder.BindVolumes(task)
}

// taskUnschedulable updates pod status of pending task
func (sc *SchedulerCache) taskUnschedulable(task *schedulingapi.TaskInfo, message string) error {
	pod := task.Pod

	condition := &v1.PodCondition{
		Type:    v1.PodScheduled,
		Status:  v1.ConditionFalse,
		Reason:  v1.PodReasonUnschedulable,
		Message: message,
	}

	if podConditionHaveUpdate(&pod.Status, condition) {
		pod = pod.DeepCopy()

		// The reason field in 'Events' should be "FailedScheduling", there is not constants defined for this in
		// k8s core, so using the same string here.
		// The reason field in PodCondition should be "Unschedulable"
		sc.Recorder.Eventf(pod, v1.EventTypeWarning, "FailedScheduling", message)
		if _, err := sc.StatusUpdater.UpdatePodCondition(pod, condition); err != nil {
			return err
		}
	} else {
		klog.V(4).Infof("task unscheduleable %s/%s, message: %s, skip by no condition update", pod.Namespace, pod.Name, message)
	}

	return nil
}

func (sc *SchedulerCache) deleteJob(job *schedulingapi.JobInfo) {
	klog.V(3).Infof("Try to delete Job <%v:%v/%v>", job.UID, job.Namespace, job.Name)

	sc.deletedJobs.AddRateLimited(job)
}

func (sc *SchedulerCache) processCleanupJob() {
	obj, shutdown := sc.deletedJobs.Get()
	if shutdown {
		return
	}

	defer sc.deletedJobs.Done(obj)

	job, found := obj.(*schedulingapi.JobInfo)
	if !found {
		klog.Errorf("Failed to convert <%v> to *JobInfo", obj)
		return
	}

	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	if schedulingapi.JobTerminated(job) {
		delete(sc.Jobs, job.UID)
		klog.V(3).Infof("Job <%v:%v/%v> was deleted.", job.UID, job.Namespace, job.Name)
	} else {
		// Retry
		sc.deleteJob(job)
	}
}

func (sc *SchedulerCache) resyncTask(task *schedulingapi.TaskInfo) {
	sc.errTasks.AddRateLimited(task)
}

func (sc *SchedulerCache) processResyncTask() {
	obj, shutdown := sc.errTasks.Get()
	if shutdown {
		return
	}

	defer sc.errTasks.Done(obj)

	task, ok := obj.(*schedulingapi.TaskInfo)
	if !ok {
		klog.Errorf("failed to convert %v to *v1.Pod", obj)
		return
	}

	if err := sc.syncTask(task); err != nil {
		klog.Errorf("Failed to sync pod <%v/%v>, retry it.", task.Namespace, task.Name)
		sc.resyncTask(task)
	}
}

// Snapshot returns the complete snapshot of the cluster from cache
func (sc *SchedulerCache) Snapshot() *schedulingapi.ClusterInfo {
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	snapshot := &schedulingapi.ClusterInfo{
		Nodes:         make(map[string]*schedulingapi.NodeInfo),
		Jobs:          make(map[schedulingapi.JobID]*schedulingapi.JobInfo),
		Queues:        make(map[schedulingapi.QueueID]*schedulingapi.QueueInfo),
		NamespaceInfo: make(map[schedulingapi.NamespaceName]*schedulingapi.NamespaceInfo),
	}

	for _, value := range sc.Nodes {
		if !value.Ready() {
			continue
		}

		snapshot.Nodes[value.Name] = value.Clone()
	}

	for _, value := range sc.Queues {
		snapshot.Queues[value.UID] = value.Clone()
	}

	var cloneJobLock sync.Mutex
	var wg sync.WaitGroup

	cloneJob := func(value *schedulingapi.JobInfo) {
		defer wg.Done()
		if value.PodGroup != nil {
			value.Priority = sc.defaultPriority

			priName := value.PodGroup.Spec.PriorityClassName
			if priorityClass, found := sc.PriorityClasses[priName]; found {
				value.Priority = priorityClass.Value
			}

			klog.V(4).Infof("The priority of job <%s/%s> is <%s/%d>",
				value.Namespace, value.Name, priName, value.Priority)
		}

		clonedJob := value.Clone()

		cloneJobLock.Lock()
		snapshot.Jobs[value.UID] = clonedJob
		cloneJobLock.Unlock()
	}

	for _, value := range sc.NamespaceCollection {
		info := value.Snapshot()
		snapshot.NamespaceInfo[info.Name] = info
		klog.V(4).Infof("Namespace %s has weight %v",
			value.Name, info.GetWeight())
	}

	for _, value := range sc.Jobs {
		// If no scheduling spec, does not handle it.
		if value.PodGroup == nil {
			klog.V(4).Infof("The scheduling spec of Job <%v:%s/%s> is nil, ignore it.",
				value.UID, value.Namespace, value.Name)

			continue
		}

		if _, found := snapshot.Queues[value.Queue]; !found {
			klog.V(3).Infof("The Queue <%v> of Job <%v/%v> does not exist, ignore it.",
				value.Queue, value.Namespace, value.Name)
			continue
		}

		wg.Add(1)
		go cloneJob(value)
	}
	wg.Wait()

	klog.V(3).Infof("There are <%d> Jobs, <%d> Queues and <%d> Nodes in total for scheduling.",
		len(snapshot.Jobs), len(snapshot.Queues), len(snapshot.Nodes))

	return snapshot
}

// String returns information about the cache in a string format
func (sc *SchedulerCache) String() string {
	sc.Mutex.Lock()
	defer sc.Mutex.Unlock()

	str := "Cache:\n"

	if len(sc.Nodes) != 0 {
		str = str + "Nodes:\n"
		for _, n := range sc.Nodes {
			str = str + fmt.Sprintf("\t %s: idle(%v) used(%v) allocatable(%v) pods(%d)\n",
				n.Name, n.Idle, n.Used, n.Allocatable, len(n.Tasks))

			i := 0
			for _, p := range n.Tasks {
				str = str + fmt.Sprintf("\t\t %d: %v\n", i, p)
				i++
			}
		}
	}

	if len(sc.Jobs) != 0 {
		str = str + "Jobs:\n"
		for _, job := range sc.Jobs {
			str = str + fmt.Sprintf("\t %s\n", job)
		}
	}

	if len(sc.NamespaceCollection) != 0 {
		str = str + "Namespaces:\n"
		for _, ns := range sc.NamespaceCollection {
			info := ns.Snapshot()
			str = str + fmt.Sprintf("\t Namespace(%s) Weight(%v)\n",
				info.Name, info.Weight)
		}
	}

	return str
}

// RecordJobStatusEvent records related events according to job status.
func (sc *SchedulerCache) RecordJobStatusEvent(job *schedulingapi.JobInfo) {
	baseErrorMessage := job.JobFitErrors
	if baseErrorMessage == "" {
		baseErrorMessage = schedulingapi.AllNodeUnavailableMsg
	}

	pgUnschedulable := job.PodGroup != nil &&
		(job.PodGroup.Status.Phase == scheduling.PodGroupUnknown ||
			job.PodGroup.Status.Phase == scheduling.PodGroupPending ||
			job.PodGroup.Status.Phase == scheduling.PodGroupInqueue)

	// If pending or unschedulable, record unschedulable event.
	if pgUnschedulable {
		msg := fmt.Sprintf("%v/%v tasks in gang unschedulable: %v",
			len(job.TaskStatusIndex[schedulingapi.Pending]),
			len(job.Tasks),
			job.FitError())
		sc.recordPodGroupEvent(job.PodGroup, v1.EventTypeWarning, string(scheduling.PodGroupUnschedulableType), msg)
	}

	// Update podCondition for tasks Allocated and Pending before job discarded
	for _, status := range []schedulingapi.TaskStatus{schedulingapi.Allocated, schedulingapi.Pending, schedulingapi.Pipelined} {
		for _, taskInfo := range job.TaskStatusIndex[status] {
			msg := baseErrorMessage
			fitError := job.NodesFitErrors[taskInfo.UID]
			if fitError != nil {
				msg = fitError.Error()
			}
			if err := sc.taskUnschedulable(taskInfo, msg); err != nil {
				klog.Errorf("Failed to update unschedulable task status <%s/%s>: %v",
					taskInfo.Namespace, taskInfo.Name, err)
			}
		}
	}
}

// UpdateJobStatus update the status of job and its tasks.
func (sc *SchedulerCache) UpdateJobStatus(job *schedulingapi.JobInfo, updatePG bool) (*schedulingapi.JobInfo, error) {
	if updatePG {
		pg, err := sc.StatusUpdater.UpdatePodGroup(job.PodGroup)
		if err != nil {
			return nil, err
		}
		job.PodGroup = pg
	}

	sc.RecordJobStatusEvent(job)

	return job, nil
}

func (sc *SchedulerCache) recordPodGroupEvent(podGroup *schedulingapi.PodGroup, eventType, reason, msg string) {
	if podGroup == nil {
		return
	}

	if podGroup.Version == schedulingapi.PodGroupVersionV1Alpha1 {
		pg := &v1alpha1.PodGroup{}
		if err := schedulingscheme.Scheme.Convert(&podGroup.PodGroup, pg, nil); err != nil {
			klog.Errorf("Error while converting PodGroup to v1alpha1.PodGroup with error: %v", err)
			return
		}

		sc.Recorder.Eventf(pg, eventType, reason, msg)
	}

	if podGroup.Version == schedulingapi.PodGroupVersionV1Alpha2 {
		pg := &v1alpha2.PodGroup{}
		if err := schedulingscheme.Scheme.Convert(&podGroup.PodGroup, pg, nil); err != nil {
			klog.Errorf("Error while converting PodGroup to v1alpha2.PodGroup with error: %v", err)
			return
		}

		sc.Recorder.Eventf(pg, eventType, reason, msg)
	}

	return
}
