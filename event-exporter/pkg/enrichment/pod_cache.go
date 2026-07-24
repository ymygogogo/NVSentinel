package enrichment

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type PodCacheConfig struct {
	LabelSelector    string
	CacheSyncTimeout time.Duration
}

type PodWatchCache struct {
	mu     sync.RWMutex
	pods   map[string]PodSummary
	byNode map[string]map[string]struct{}
	synced cache.InformerSynced
}

func NewKubernetesPodWatchCache(ctx context.Context, cfg PodCacheConfig) (*PodWatchCache, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("create in-cluster kubernetes config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return NewPodWatchCache(ctx, clientset, cfg)
}

func NewPodWatchCache(ctx context.Context, clientset kubernetes.Interface, cfg PodCacheConfig) (*PodWatchCache, error) {
	podCache := &PodWatchCache{
		pods:   map[string]PodSummary{},
		byNode: map[string]map[string]struct{}{},
	}

	factory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		0,
		informers.WithNamespace(metav1.NamespaceAll),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = cfg.LabelSelector
		}),
	)
	informer := factory.Core().V1().Pods().Informer()
	podCache.synced = informer.HasSynced

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			podCache.upsert(obj)
		},
		UpdateFunc: func(_, newObj any) {
			podCache.upsert(newObj)
		},
		DeleteFunc: func(obj any) {
			podCache.delete(obj)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("add pod event handler: %w", err)
	}

	factory.Start(ctx.Done())

	timeout := cfg.CacheSyncTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	syncCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), informer.HasSynced) {
		return nil, fmt.Errorf("timed out waiting for pod cache sync")
	}

	return podCache, nil
}

func (c *PodWatchCache) GetPods(_ context.Context, query Query) ([]PodSummary, error) {
	if c == nil {
		return nil, fmt.Errorf("pod cache is not configured")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	uids := c.byNode[query.NodeName]
	pods := make([]PodSummary, 0, len(uids))
	for uid := range uids {
		pod, ok := c.pods[uid]
		if ok {
			pods = append(pods, pod)
		}
	}
	return pods, nil
}

func (c *PodWatchCache) upsert(obj any) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod.UID == "" || pod.Spec.NodeName == "" {
		return
	}
	uid := string(pod.UID)
	summary := PodSummary{
		Namespace:   pod.Namespace,
		Name:        pod.Name,
		NodeName:    pod.Spec.NodeName,
		Labels:      copyMap(pod.Labels),
		Annotations: copyMap(pod.Annotations),
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if old, ok := c.pods[uid]; ok && old.NodeName != "" && old.NodeName != summary.NodeName {
		delete(c.byNode[old.NodeName], uid)
	}
	c.pods[uid] = summary
	if c.byNode[summary.NodeName] == nil {
		c.byNode[summary.NodeName] = map[string]struct{}{}
	}
	c.byNode[summary.NodeName][uid] = struct{}{}
}

func (c *PodWatchCache) delete(obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod.UID == "" {
		return
	}
	uid := string(pod.UID)

	c.mu.Lock()
	defer c.mu.Unlock()
	old, ok := c.pods[uid]
	if !ok {
		return
	}
	delete(c.pods, uid)
	if old.NodeName != "" {
		delete(c.byNode[old.NodeName], uid)
	}
}

func copyMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]string, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}
