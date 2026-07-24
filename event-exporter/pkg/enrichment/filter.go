package enrichment

import "regexp"

type filter struct {
	include         *regexp.Regexp
	exclude         *regexp.Regexp
	labelAllow      map[string]struct{}
	annotationAllow map[string]struct{}
}

func newFilter(includePattern, excludePattern string, labels, annotations []string) (*filter, error) {
	var include *regexp.Regexp
	var exclude *regexp.Regexp
	var err error

	if includePattern != "" {
		include, err = regexp.Compile(includePattern)
		if err != nil {
			return nil, err
		}
	}
	if excludePattern != "" {
		exclude, err = regexp.Compile(excludePattern)
		if err != nil {
			return nil, err
		}
	}

	return &filter{
		include:         include,
		exclude:         exclude,
		labelAllow:      stringSet(labels),
		annotationAllow: stringSet(annotations),
	}, nil
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func (f *filter) allowNamespace(namespace string) bool {
	if f.include != nil && !f.include.MatchString(namespace) {
		return false
	}
	if f.exclude != nil && f.exclude.MatchString(namespace) {
		return false
	}
	return true
}

func (f *filter) normalizePods(pods []PodSummary) []PodSummary {
	out := make([]PodSummary, 0, len(pods))
	for _, pod := range pods {
		if pod.Namespace == "" || pod.Name == "" {
			continue
		}
		if !f.allowNamespace(pod.Namespace) {
			continue
		}
		pod.Labels = allowMap(pod.Labels, f.labelAllow)
		pod.Annotations = allowMap(pod.Annotations, f.annotationAllow)
		out = append(out, pod)
	}
	return out
}

func allowMap(input map[string]string, allow map[string]struct{}) map[string]string {
	if len(input) == 0 || len(allow) == 0 {
		return nil
	}
	out := make(map[string]string)
	for key, value := range input {
		if _, ok := allow[key]; ok {
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
