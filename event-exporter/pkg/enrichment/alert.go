package enrichment

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type WebhookAlerter struct {
	url        string
	httpClient *http.Client
	maxRetries int
}

func NewWebhookAlerter(url string, timeout time.Duration, maxRetries int) *WebhookAlerter {
	if timeout == 0 {
		timeout = 3 * time.Second
	}
	return &WebhookAlerter{
		url:        url,
		httpClient: &http.Client{Timeout: timeout},
		maxRetries: maxRetries,
	}
}

func (a *WebhookAlerter) Alert(ctx context.Context, alert MissingPodContextAlert) error {
	if a == nil || a.url == "" {
		return nil
	}

	title := "NVSentinel event published with pod context enrichment failed"
	if alert.PodSource == "sink" {
		title = "NVSentinel event publish failed after max retries"
	}
	payload := map[string]any{
		"msgtype": "markdown",
		"markdown": map[string]string{
			"content": fmt.Sprintf(
				"%s\n> cluster: %s\n> node: %s\n> check: %s\n> generated_at: %s\n> reason: %s",
				title,
				alert.Cluster,
				alert.NodeName,
				alert.CheckName,
				alert.EventTime.UTC().Format(time.RFC3339Nano),
				alert.Reason,
			),
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal webhook alert: %w", err)
	}

	attempts := a.maxRetries + 1
	for attempt := 0; attempt < attempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("create webhook request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := a.httpClient.Do(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			_ = resp.Body.Close()
			return nil
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		if err != nil && attempt == attempts-1 {
			return fmt.Errorf("send webhook alert: %w", err)
		}
	}

	return fmt.Errorf("send webhook alert: retries exhausted")
}
