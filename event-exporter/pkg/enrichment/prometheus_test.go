package enrichment

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPrometheusProviderReturnsPodSummaries(t *testing.T) {
	eventTime := time.Date(2026, 7, 29, 10, 6, 59, 604634855, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/query_range" {
			t.Fatalf("path = %s, want /api/v1/query_range", r.URL.Path)
		}
		if got, want := r.URL.Query().Get("start"), eventTime.Add(-2*time.Minute).Format(time.RFC3339Nano); got != want {
			t.Fatalf("start = %q, want %q", got, want)
		}
		if got, want := r.URL.Query().Get("end"), eventTime.Add(2*time.Minute).Format(time.RFC3339Nano); got != want {
			t.Fatalf("end = %q, want %q", got, want)
		}
		if got := r.URL.Query().Get("step"); got != "15s" {
			t.Fatalf("step = %q, want 15s", got)
		}
		_, _ = w.Write([]byte(`{
			"status":"success",
			"data":{"resultType":"matrix","result":[
				{"metric":{"namespace":"cci-a","pod":"train-1","node":"gpu-node-1","label_tenant_id":"tenant-a","label_task_id":"task-1","label_secret":"drop","annotation_platform_datacanvas_com_order_id":"order-1","annotation_platform_datacanvas_com_secret":"drop"},"values":[[1710000000,"1"],[1710000015,"1"]]},
				{"metric":{"namespace":"cci-a","pod":"train-1","node":"gpu-node-1","label_tenant_id":"tenant-a","label_task_id":"task-1"},"values":[[1710000030,"1"]]},
				{"metric":{"namespace":"kube-system","pod":"ignored","node":"gpu-node-1","label_tenant_id":"tenant-a"},"values":[[1710000000,"1"]]}
			]}
		}`))
	}))
	defer server.Close()

	provider := NewPrometheusProvider(PrometheusConfig{
		Endpoint:            server.URL,
		Timeout:             time.Second,
		QueryLookback:       2 * time.Minute,
		QueryRangeStep:      15 * time.Second,
		LabelAllowlist:      []string{"tenant_id", "task_id"},
		AnnotationAllowlist: []string{"platform.datacanvas.com/order-id"},
	})

	pods, err := provider.GetPods(context.Background(), Query{NodeName: "gpu-node-1", EventTime: eventTime})
	if err != nil {
		t.Fatalf("GetPods() error = %v", err)
	}
	if len(pods) != 2 {
		t.Fatalf("len(pods) = %d, want 2 before namespace filtering and after pod dedupe", len(pods))
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
	if pods[0].Annotations["platform.datacanvas.com/order-id"] != "order-1" {
		t.Fatalf("annotations = %+v, want order id", pods[0].Annotations)
	}
	if _, ok := pods[0].Annotations["platform.datacanvas.com/secret"]; ok {
		t.Fatalf("secret annotation should not be included: %+v", pods[0].Annotations)
	}
}

func TestPrometheusProviderUsesConfiguredQueryTemplate(t *testing.T) {
	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("query")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
	defer server.Close()

	provider := NewPrometheusProvider(PrometheusConfig{
		Endpoint:            server.URL,
		Timeout:             time.Second,
		QueryLookback:       10 * time.Minute,
		LabelAllowlist:      []string{"tenant_id", "task_id"},
		AnnotationAllowlist: []string{"platform.datacanvas.com/order-id"},
		QueryTemplate:       `custom_pod_query{node="{{ .NodeName }}", labels="{{ .GroupLeftLabels }}", annotations="{{ .GroupLeftAnnotations }}"}[{{ .QueryLookback }}:]`,
	})

	_, err := provider.GetPods(context.Background(), Query{NodeName: "gpu-node-1", EventTime: time.Now().UTC()})
	if err != nil {
		t.Fatalf("GetPods() error = %v", err)
	}
	want := `custom_pod_query{node="gpu-node-1", labels="label_tenant_id, label_task_id", annotations="annotation_platform_datacanvas_com_order_id"}[10m0s:]`
	if gotQuery != want {
		t.Fatalf("query = %q, want %q", gotQuery, want)
	}
}
