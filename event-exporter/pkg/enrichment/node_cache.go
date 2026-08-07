package enrichment

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type NodeCacheConfig struct {
	CacheSyncTimeout time.Duration
}

type NodeWatchCache struct {
	mu     sync.RWMutex
	nodes  map[string]NodeSummary
	synced cache.InformerSynced
}

func NewKubernetesNodeWatchCache(ctx context.Context, cfg NodeCacheConfig) (*NodeWatchCache, error) {
	restConfig, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("create in-cluster kubernetes config: %w", err)
	}
	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("create kubernetes client: %w", err)
	}
	return NewNodeWatchCache(ctx, clientset, cfg)
}

func NewNodeWatchCache(ctx context.Context, clientset kubernetes.Interface, cfg NodeCacheConfig) (*NodeWatchCache, error) {
	nodeCache := &NodeWatchCache{
		nodes: map[string]NodeSummary{},
	}

	factory := informers.NewSharedInformerFactory(clientset, 0)
	informer := factory.Core().V1().Nodes().Informer()
	nodeCache.synced = informer.HasSynced

	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			nodeCache.upsert(obj)
		},
		UpdateFunc: func(_, newObj any) {
			nodeCache.upsert(newObj)
		},
		DeleteFunc: func(obj any) {
			nodeCache.delete(obj)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("add node event handler: %w", err)
	}

	factory.Start(ctx.Done())

	timeout := cfg.CacheSyncTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	syncCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if !cache.WaitForCacheSync(syncCtx.Done(), informer.HasSynced) {
		return nil, fmt.Errorf("timed out waiting for node cache sync")
	}

	return nodeCache, nil
}

func (c *NodeWatchCache) GetNode(_ context.Context, name string) (*NodeSummary, error) {
	if c == nil {
		return nil, fmt.Errorf("node cache is not configured")
	}
	if name == "" {
		return nil, nil
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	node, ok := c.nodes[name]
	if !ok {
		return nil, nil
	}
	return &node, nil
}

func (c *NodeWatchCache) upsert(obj any) {
	node, ok := obj.(*corev1.Node)
	if !ok || node.Name == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.nodes[node.Name] = NodeSummary{
		Name:   node.Name,
		Labels: copyMap(node.Labels),
	}
}

func (c *NodeWatchCache) delete(obj any) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	node, ok := obj.(*corev1.Node)
	if !ok || node.Name == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.nodes, node.Name)
}
