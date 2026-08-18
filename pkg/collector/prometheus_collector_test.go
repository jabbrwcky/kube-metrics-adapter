package collector

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestNewPrometheusCollector(t *testing.T) {
	for _, tc := range []struct {
		msg string
		hpa *autoscalingv2.HorizontalPodAutoscaler
		// config        *MetricConfig
		valid         bool
		expectedQuery string
	}{
		{
			msg: "valid external metric configuration should work",
			hpa: &autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"metric-config.external.rps.prometheus/query": "sum(rate(rps[1m]))",
					},
				},
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					Metrics: []autoscalingv2.MetricSpec{
						{
							Type: autoscalingv2.ExternalMetricSourceType,
							External: &autoscalingv2.ExternalMetricSource{
								Metric: autoscalingv2.MetricIdentifier{
									Name: "rps",
									Selector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"type": "prometheus"},
									},
								},
							},
						},
					},
				},
			},
			expectedQuery: "sum(rate(rps[1m]))",
		},
		{
			msg: "missing query for external metric should not work",
			hpa: &autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"metric-config.external.rps.prometheus/not-query": "sum(rate(rps[1m]))",
					},
				},
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					Metrics: []autoscalingv2.MetricSpec{
						{
							Type: autoscalingv2.ExternalMetricSourceType,
							External: &autoscalingv2.ExternalMetricSource{
								Metric: autoscalingv2.MetricIdentifier{
									Name: "rps",
									Selector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"type": "prometheus"},
									},
								},
							},
						},
					},
				},
			},
			expectedQuery: "",
		},
		{
			msg: "valid legacy external metric configuration should work",
			hpa: &autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"metric-config.external.prometheus-query.prometheus/rps": "sum(rate(rps[1m]))",
					},
				},
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					Metrics: []autoscalingv2.MetricSpec{
						{
							Type: autoscalingv2.ExternalMetricSourceType,
							External: &autoscalingv2.ExternalMetricSource{
								Metric: autoscalingv2.MetricIdentifier{
									Name: PrometheusMetricNameLegacy,
									Selector: &metav1.LabelSelector{
										MatchLabels: map[string]string{prometheusQueryNameLabelKey: "rps"},
									},
								},
							},
						},
					},
				},
			},
			expectedQuery: "sum(rate(rps[1m]))",
		},
		{
			msg: "invalid legacy external metric configuration with wrong query name should not work",
			hpa: &autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"metric-config.external.prometheus-query.prometheus/rps": "sum(rate(rps[1m]))",
					},
				},
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					Metrics: []autoscalingv2.MetricSpec{
						{
							Type: autoscalingv2.ExternalMetricSourceType,
							External: &autoscalingv2.ExternalMetricSource{
								Metric: autoscalingv2.MetricIdentifier{
									Name: PrometheusMetricNameLegacy,
									Selector: &metav1.LabelSelector{
										MatchLabels: map[string]string{prometheusQueryNameLabelKey: "not-rps"},
									},
								},
							},
						},
					},
				},
			},
			expectedQuery: "",
		},
		{
			msg: "invalid legacy external metric configuration with missing selector should not work",
			hpa: &autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"metric-config.external.prometheus-query.prometheus/rps": "sum(rate(rps[1m]))",
					},
				},
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					Metrics: []autoscalingv2.MetricSpec{
						{
							Type: autoscalingv2.ExternalMetricSourceType,
							External: &autoscalingv2.ExternalMetricSource{
								Metric: autoscalingv2.MetricIdentifier{
									Name: PrometheusMetricNameLegacy,
								},
							},
						},
					},
				},
			},
			expectedQuery: "",
		},
		{
			msg: "invalid legacy external metric configuration with missing query name should not work",
			hpa: &autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"metric-config.external.prometheus-query.prometheus/rps": "sum(rate(rps[1m]))",
					},
				},
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					Metrics: []autoscalingv2.MetricSpec{
						{
							Type: autoscalingv2.ExternalMetricSourceType,
							External: &autoscalingv2.ExternalMetricSource{
								Metric: autoscalingv2.MetricIdentifier{
									Name: PrometheusMetricNameLegacy,
									Selector: &metav1.LabelSelector{
										MatchLabels: map[string]string{"not-query-name": "not-rps"},
									},
								},
							},
						},
					},
				},
			},
			expectedQuery: "",
		},
	} {
		t.Run(tc.msg, func(t *testing.T) {
			collectorFactory := NewCollectorFactory()
			promPlugin, err := NewPrometheusCollectorPlugin(nil, "http://prometheus", "", map[string]string{}, map[string]string{})
			require.NoError(t, err)
			collectorFactory.RegisterExternalCollector([]string{PrometheusMetricType, PrometheusMetricNameLegacy}, promPlugin)
			configs, err := ParseHPAMetrics(tc.hpa)
			require.NoError(t, err)
			require.Len(t, configs, 1)

			collector, err := collectorFactory.NewCollector(context.Background(), tc.hpa, configs[0], 0)
			if tc.expectedQuery != "" {
				require.NoError(t, err)
				c, ok := collector.(*PrometheusCollector)
				require.True(t, ok)
				require.Equal(t, tc.expectedQuery, c.query)
			} else {
				require.Error(t, err)
			}
		})
	}
}

// fakePromAPI implements only Query; the embedded interface panics on anything
// else, which keeps the surface of these tests small.
type fakePromAPI struct {
	promv1.API
	value    model.Value
	warnings promv1.Warnings
	err      error
}

func (f fakePromAPI) Query(_ context.Context, _ string, _ time.Time, _ ...promv1.Option) (model.Value, promv1.Warnings, error) {
	return f.value, f.warnings, f.err
}

func TestPrometheusCollectorGetMetrics(t *testing.T) {
	for _, tc := range []struct {
		msg           string
		value         model.Value
		warnings      promv1.Warnings
		queryErr      error
		expectedError error
		expectedValue int64
	}{
		{
			msg:           "single sample vector is collected",
			value:         model.Vector{{Value: 42}},
			expectedValue: 42,
		},
		{
			msg:           "scalar is collected",
			value:         &model.Scalar{Value: 7},
			expectedValue: 7,
		},
		{
			msg:           "warnings do not prevent collection",
			value:         model.Vector{{Value: 3}},
			warnings:      promv1.Warnings{"too many samples"},
			expectedValue: 3,
		},
		{
			msg:           "empty vector yields no result",
			value:         model.Vector{},
			expectedError: &NoResultError{query: "some_query"},
		},
		{
			msg:           "multi sample vector is ambiguous",
			value:         model.Vector{{Value: 1}, {Value: 2}},
			expectedError: &AmbiguousResultError{query: "some_query", samples: 2},
		},
		{
			msg:           "NaN scalar yields no result",
			value:         &model.Scalar{Value: model.SampleValue(math.NaN())},
			expectedError: &NoResultError{query: "some_query"},
		},
		{
			// a subquery returns a matrix from the instant query endpoint; it
			// must not be silently reported as 0
			msg:           "matrix is not collected as zero",
			value:         model.Matrix{{Values: []model.SamplePair{{Value: 5}}}},
			expectedError: &NoResultError{query: "some_query"},
		},
		{
			msg:           "string is not collected as zero",
			value:         &model.String{Value: "foo"},
			expectedError: &NoResultError{query: "some_query"},
		},
		{
			msg:           "query error is propagated",
			queryErr:      errors.New("prometheus unreachable"),
			expectedError: errors.New("prometheus unreachable"),
		},
	} {
		t.Run(tc.msg, func(t *testing.T) {
			c := &PrometheusCollector{
				promAPI:    fakePromAPI{value: tc.value, warnings: tc.warnings, err: tc.queryErr},
				query:      "some_query",
				metricType: autoscalingv2.ExternalMetricSourceType,
				metric: autoscalingv2.MetricIdentifier{
					Name:     "rps",
					Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"type": "prometheus"}},
				},
				hpa: &autoscalingv2.HorizontalPodAutoscaler{},
			}

			metrics, err := c.GetMetrics(context.Background())
			if tc.expectedError != nil {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.expectedError.Error())
				return
			}

			require.NoError(t, err)
			require.Len(t, metrics, 1)
			require.Equal(t, tc.expectedValue, metrics[0].External.Value.Value())
		})
	}
}
