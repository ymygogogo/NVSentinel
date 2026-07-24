package enrichment

import "encoding/json"

func limitPayloadPods(pods []PodSummary, maxBytes int) ([]PodSummary, bool) {
	if maxBytes <= 0 || len(pods) == 0 {
		return pods, false
	}
	current := pods
	for len(current) > 0 {
		encoded, err := json.Marshal(current)
		if err != nil {
			return current, false
		}
		if len(encoded) <= maxBytes {
			return current, len(current) != len(pods)
		}
		current = current[:len(current)-1]
	}
	return nil, len(pods) > 0
}
