package enrichment

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type PrometheusConfig struct {
	Endpoint             string
	Timeout              time.Duration
	QueryLookback        time.Duration
	LabelAllowlist       []string
	MaxConcurrentQueries int
	CacheTTL             time.Duration
}

type PrometheusProvider struct {
	cfg        PrometheusConfig
	httpClient *http.Client
	labelAllow map[string]struct{}
	sem        chan struct{}
	cacheTTL   time.Duration
	mu         sync.Mutex
	cache      map[string]prometheusCacheEntry
}

type prometheusCacheEntry struct {
	expiresAt time.Time
	pods      []PodSummary
}

func NewPrometheusProvider(cfg PrometheusConfig) *PrometheusProvider {
	if cfg.Timeout == 0 {
		cfg.Timeout = 3 * time.Second
	}
	if cfg.QueryLookback == 0 {
		cfg.QueryLookback = 5 * time.Minute
	}
	if cfg.MaxConcurrentQueries <= 0 {
		cfg.MaxConcurrentQueries = 5
	}
	provider := &PrometheusProvider{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: cfg.Timeout},
		labelAllow: stringSet(cfg.LabelAllowlist),
		sem:        make(chan struct{}, cfg.MaxConcurrentQueries),
		cacheTTL:   cfg.CacheTTL,
		cache:      map[string]prometheusCacheEntry{},
	}
	return provider
}

func (p *PrometheusProvider) GetPods(ctx context.Context, query Query) ([]PodSummary, error) {
	if p == nil || p.cfg.Endpoint == "" {
		return nil, fmt.Errorf("prometheus endpoint is not configured")
	}
	cacheKey := p.cacheKey(query)
	if pods, ok := p.getCached(cacheKey); ok {
		return pods, nil
	}

	select {
	case p.sem <- struct{}{}:
		defer func() { <-p.sem }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	promQL := p.query(query.NodeName)
	endpoint, err := url.Parse(strings.TrimRight(p.cfg.Endpoint, "/") + "/api/v1/query")
	if err != nil {
		return nil, fmt.Errorf("parse prometheus endpoint: %w", err)
	}
	values := endpoint.Query()
	values.Set("query", promQL)
	values.Set("time", query.EventTime.UTC().Format(time.RFC3339Nano))
	endpoint.RawQuery = values.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create prometheus request: %w", err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query prometheus: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("prometheus returned status %d", resp.StatusCode)
	}

	var payload prometheusResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, fmt.Errorf("decode prometheus response: %w", err)
	}
	if payload.Status != "success" {
		return nil, fmt.Errorf("prometheus status %q", payload.Status)
	}

	pods := make([]PodSummary, 0, len(payload.Data.Result))
	for _, result := range payload.Data.Result {
		pod := result.toPodSummary(p.labelAllow)
		if pod.Namespace == "" || pod.Name == "" {
			continue
		}
		if pod.NodeName == "" {
			pod.NodeName = query.NodeName
		}
		pods = append(pods, pod)
	}

	p.setCached(cacheKey, pods)

	return pods, nil
}

func (p *PrometheusProvider) cacheKey(query Query) string {
	return query.NodeName + "|" + query.EventTime.UTC().Format(time.RFC3339Nano)
}

func (p *PrometheusProvider) getCached(key string) ([]PodSummary, bool) {
	if p.cacheTTL <= 0 {
		return nil, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.cache[key]
	if !ok || time.Now().After(entry.expiresAt) {
		delete(p.cache, key)
		return nil, false
	}
	return append([]PodSummary(nil), entry.pods...), true
}

func (p *PrometheusProvider) setCached(key string, pods []PodSummary) {
	if p.cacheTTL <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cache[key] = prometheusCacheEntry{
		expiresAt: time.Now().Add(p.cacheTTL),
		pods:      append([]PodSummary(nil), pods...),
	}
}

func (p *PrometheusProvider) query(nodeName string) string {
	lookback := p.cfg.QueryLookback.String()
	groupLeftLabels := p.groupLeftLabels()
	return fmt.Sprintf(`last_over_time((kube_pod_info{node=%q} * on(namespace, pod) group_left(%s) kube_pod_labels)[%s:])`, nodeName, groupLeftLabels, lookback)
}

func (p *PrometheusProvider) groupLeftLabels() string {
	labels := make([]string, 0, len(p.cfg.LabelAllowlist))
	for _, label := range p.cfg.LabelAllowlist {
		labels = append(labels, "label_"+label)
	}
	return strings.Join(labels, ", ")
}

type prometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []prometheusResult `json:"result"`
	} `json:"data"`
}

type prometheusResult struct {
	Metric map[string]string `json:"metric"`
}

func (r prometheusResult) toPodSummary(labelAllow map[string]struct{}) PodSummary {
	pod := PodSummary{
		Namespace: r.Metric["namespace"],
		Name:      firstNonEmpty(r.Metric["pod"], r.Metric["pod_name"]),
		NodeName:  firstNonEmpty(r.Metric["node"], r.Metric["node_name"]),
		Labels:    map[string]string{},
	}
	for key, value := range r.Metric {
		if !strings.HasPrefix(key, "label_") {
			continue
		}
		labelName := strings.TrimPrefix(key, "label_")
		if _, ok := labelAllow[labelName]; ok {
			pod.Labels[labelName] = value
		}
	}
	if len(pod.Labels) == 0 {
		pod.Labels = nil
	}
	return pod
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
