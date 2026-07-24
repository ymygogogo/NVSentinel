package enrichment

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPrometheusProviderReturnsPodSummaries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query" {
			t.Fatalf("path = %s, want /api/v1/query", r.URL.Path)
		}
		if r.URL.Query().Get("time") == "" {
			t.Fatal("time query parameter is empty")
		}
		_, _ = w.Write([]byte(`{
			"status":"success",
			"data":{"resultType":"vector","result":[
				{"metric":{"namespace":"cci-a","pod":"train-1","node":"gpu-node-1","label_tenant_id":"tenant-a","label_task_id":"task-1","label_secret":"drop"},"value":[1710000000,"1"]},
				{"metric":{"namespace":"kube-system","pod":"ignored","node":"gpu-node-1","label_tenant_id":"tenant-a"},"value":[1710000000,"1"]}
			]}
		}`))
	}))
	defer server.Close()

	provider := NewPrometheusProvider(PrometheusConfig{
		Endpoint:       server.URL,
		Timeout:        time.Second,
		QueryLookback:  5 * time.Minute,
		LabelAllowlist: []string{"tenant_id", "task_id"},
	})

	pods, err := provider.GetPods(context.Background(), Query{NodeName: "gpu-node-1", EventTime: time.Now().UTC()})
	if err != nil {
		t.Fatalf("GetPods() error = %v", err)
	}
	if len(pods) != 2 {
		t.Fatalf("len(pods) = %d, want 2 before namespace filtering", len(pods))
	}
	if pods[0].Namespace != "cci-a" || pods[0].Name != "train-1" || pods[0].NodeName != "gpu-node-1" {
		t.Fatalf("pod[0] = %+v", pods[0])
	}
	if pods[0].Labels["tenant_id"] != "tenant-a" || pods[0].Labels["task_id"] != "task-1" {
		t.Fatalf("labels = %+v", pods[0].Labels)
	}
	if _, ok := pods[0].Labels["secret"]; ok {
		t.Fatalf("secret label should not be included: %+v", pods[0].Labels)
	}
}
