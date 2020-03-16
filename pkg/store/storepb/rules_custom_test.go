// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/testutil"
	"github.com/thanos-io/thanos/pkg/testutil/testpromcompatibility"
)

func TestJSONUnmarshalMarshal(t *testing.T) {
	now := time.Now()
	twoHoursAgo := now.Add(2 * time.Hour)

	for _, tcase := range []struct {
		name  string
		input *testpromcompatibility.RuleDiscovery

		expectedProto      *RuleGroups
		expectedErr        error
		expectedJSONOutput string // If empty, expected same one as marshaled input.
	}{
		{
			name:          "Empty JSON",
			input:         &testpromcompatibility.RuleDiscovery{},
			expectedProto: &RuleGroups{},
		},
		{
			name: "one empty group",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name:                              "group1",
						File:                              "file1.yml",
						Interval:                          2442,
						LastEvaluation:                    now,
						EvaluationTime:                    2.1,
						DeprecatedPartialResponseStrategy: "WARN",
						PartialResponseStrategy:           "ABORT",
					},
				},
			},
			expectedProto: &RuleGroups{
				Groups: []*RuleGroup{
					{
						Name:                              "group1",
						File:                              "file1.yml",
						Interval:                          2442,
						LastEvaluation:                    now,
						EvaluationDurationSeconds:         2.1,
						DeprecatedPartialResponseStrategy: PartialResponseStrategy_WARN,
						PartialResponseStrategy:           PartialResponseStrategy_ABORT,
					},
				},
			},
		},
		{
			name: "one group with one empty group",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{},
				},
			},
			expectedProto: &RuleGroups{
				Groups: []*RuleGroup{
					{
						DeprecatedPartialResponseStrategy: PartialResponseStrategy_WARN,
						PartialResponseStrategy:           PartialResponseStrategy_WARN,
					},
				},
			},
			// Different than input due to default enum fields.
			expectedJSONOutput: `{"groups":[{"name":"","file":"","rules":null,"interval":0,"evaluationTime":0,"lastEvaluation":"0001-01-01T00:00:00Z","partial_response_strategy":"WARN","partialResponseStrategy":"WARN"}]}`,
		},
		{
			name: "one valid group, with 1 with no rule type",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name: "group1",
						Rules: []testpromcompatibility.Rule{
							testpromcompatibility.RecordingRule{
								Name: "recording1",
							},
						},
						File:                              "file1.yml",
						Interval:                          2442,
						LastEvaluation:                    now,
						EvaluationTime:                    2.1,
						DeprecatedPartialResponseStrategy: "WARN",
						PartialResponseStrategy:           "ABORT",
					},
				},
			},
			expectedErr: errors.New("rule: no type field provided: {\"name\":\"recording1\",\"query\":\"\",\"health\":\"\",\"evaluationTime\":0,\"lastEvaluation\":\"0001-01-01T00:00:00Z\",\"type\":\"\"}"),
		},
		{
			name: "one valid group, with 1 rule with invalid rule type",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name: "group1",
						Rules: []testpromcompatibility.Rule{
							testpromcompatibility.RecordingRule{
								Name: "recording1",
								Type: "wrong",
							},
						},
						File:                              "file1.yml",
						Interval:                          2442,
						LastEvaluation:                    now,
						EvaluationTime:                    2.1,
						DeprecatedPartialResponseStrategy: "WARN",
						PartialResponseStrategy:           "ABORT",
					},
				},
			},
			expectedErr: errors.New("rule: unknown type field provided wrong; {\"name\":\"recording1\",\"query\":\"\",\"health\":\"\",\"evaluationTime\":0,\"lastEvaluation\":\"0001-01-01T00:00:00Z\",\"type\":\"wrong\"}"),
		},
		{
			name: "one valid group, with 1 rule with invalid alert state",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name: "group1",
						Rules: []testpromcompatibility.Rule{
							testpromcompatibility.AlertingRule{
								Name:  "alert1",
								Type:  RuleAlertingType,
								State: "sdfsdf",
							},
						},
						File:                              "file1.yml",
						Interval:                          2442,
						LastEvaluation:                    now,
						EvaluationTime:                    2.1,
						DeprecatedPartialResponseStrategy: "WARN",
						PartialResponseStrategy:           "ABORT",
					},
				},
			},
			expectedErr: errors.New("rule: alerting rule unmarshal: {\"state\":\"sdfsdf\",\"name\":\"alert1\",\"query\":\"\",\"duration\":0,\"labels\":{},\"annotations\":{},\"alerts\":null,\"health\":\"\",\"evaluationTime\":0,\"lastEvaluation\":\"0001-01-01T00:00:00Z\",\"type\":\"alerting\"}: unknown alertState: \"sdfsdf\""),
		},
		{
			name: "one group with WRONG partial response fields",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name:                    "group1",
						File:                    "file1.yml",
						Interval:                2442,
						LastEvaluation:          now,
						EvaluationTime:          2.1,
						PartialResponseStrategy: "asdfsdfsdfsd",
					},
				},
			},
			expectedErr: errors.New("unknown partialResponseStrategy: \"asdfsdfsdfsd\""),
		},
		{
			name: "one valid group, with 1 rule and alert each and second empty group.",
			input: &testpromcompatibility.RuleDiscovery{
				RuleGroups: []*testpromcompatibility.RuleGroup{
					{
						Name: "group1",
						Rules: []testpromcompatibility.Rule{
							testpromcompatibility.RecordingRule{
								Type:  RuleRecordingType,
								Query: "up",
								Name:  "recording1",
								Labels: labels.Labels{
									{Name: "a", Value: "b"},
									{Name: "c", Value: "d"},
									{Name: "a", Value: "b"}, // Kind of invalid, but random one will be chosen.
								},
								LastError:      "2",
								Health:         "health",
								LastEvaluation: now.Add(-2 * time.Minute),
								EvaluationTime: 2.6,
							},
							testpromcompatibility.AlertingRule{
								Type:  RuleAlertingType,
								Name:  "alert1",
								Query: "up == 0",
								Labels: labels.Labels{
									{Name: "a2", Value: "b2"},
									{Name: "c2", Value: "d2"},
								},
								Annotations: labels.Labels{
									{Name: "ann1", Value: "ann44"},
									{Name: "ann2", Value: "ann33"},
								},
								Health: "health2",
								Alerts: []*testpromcompatibility.Alert{
									{
										Labels: labels.Labels{
											{Name: "instance1", Value: "1"},
										},
										Annotations: labels.Labels{
											{Name: "annotation1", Value: "2"},
										},
										State:                   "INACTIVE",
										ActiveAt:                nil,
										Value:                   "1",
										PartialResponseStrategy: "WARN",
									},
									{
										Labels:                  nil,
										Annotations:             nil,
										State:                   "FIRING",
										ActiveAt:                &twoHoursAgo,
										Value:                   "2143",
										PartialResponseStrategy: "ABORT",
									},
								},
								LastError:      "1",
								Duration:       60,
								State:          "PENDING",
								LastEvaluation: now.Add(-1 * time.Minute),
								EvaluationTime: 1.1,
							},
						},
						File:                              "file1.yml",
						Interval:                          2442,
						LastEvaluation:                    now,
						EvaluationTime:                    2.1,
						DeprecatedPartialResponseStrategy: "WARN",
						PartialResponseStrategy:           "ABORT",
					},
					{
						Name:                              "group2",
						File:                              "file2.yml",
						Interval:                          242342442,
						LastEvaluation:                    now.Add(40 * time.Hour),
						EvaluationTime:                    21244.1,
						DeprecatedPartialResponseStrategy: "ABORT",
						PartialResponseStrategy:           "ABORT",
					},
				},
			},
			expectedProto: &RuleGroups{
				Groups: []*RuleGroup{
					{
						Name: "group1",
						Rules: []*Rule{
							{
								Result: &Rule_Recording{
									Recording: &RecordingRule{
										Query: "up",
										Name:  "recording1",
										Labels: &PromLabels{
											Labels: []Label{
												{Name: "a", Value: "b"},
												{Name: "c", Value: "d"},
											},
										},
										LastError:                 "2",
										Health:                    "health",
										LastEvaluation:            now.Add(-2 * time.Minute),
										EvaluationDurationSeconds: 2.6,
									},
								},
							},
							{
								Result: &Rule_Alert{
									Alert: &Alert{
										Name:  "alert1",
										Query: "up == 0",
										Labels: &PromLabels{
											Labels: []Label{
												{Name: "a2", Value: "b2"},
												{Name: "c2", Value: "d2"},
											},
										},
										Annotations: &PromLabels{
											Labels: []Label{
												{Name: "ann1", Value: "ann44"},
												{Name: "ann2", Value: "ann33"},
											},
										},
										Alerts: []*AlertInstance{
											{
												Labels: &PromLabels{
													Labels: []Label{
														{Name: "instance1", Value: "1"},
													},
												},
												Annotations: &PromLabels{
													Labels: []Label{
														{Name: "annotation1", Value: "2"},
													},
												},
												State:                   AlertState_INACTIVE,
												ActiveAt:                nil,
												Value:                   "1",
												PartialResponseStrategy: PartialResponseStrategy_WARN,
											},
											{
												Labels:                  &PromLabels{},
												Annotations:             &PromLabels{},
												State:                   AlertState_FIRING,
												ActiveAt:                &twoHoursAgo,
												Value:                   "2143",
												PartialResponseStrategy: PartialResponseStrategy_ABORT,
											},
										},
										DurationSeconds:           60,
										State:                     AlertState_PENDING,
										LastError:                 "1",
										Health:                    "health2",
										LastEvaluation:            now.Add(-1 * time.Minute),
										EvaluationDurationSeconds: 1.1,
									},
								},
							},
						},
						File:                              "file1.yml",
						Interval:                          2442,
						LastEvaluation:                    now,
						EvaluationDurationSeconds:         2.1,
						DeprecatedPartialResponseStrategy: PartialResponseStrategy_WARN,
						PartialResponseStrategy:           PartialResponseStrategy_ABORT,
					},
					{
						Name:                              "group2",
						File:                              "file2.yml",
						Interval:                          242342442,
						LastEvaluation:                    now.Add(40 * time.Hour),
						EvaluationDurationSeconds:         21244.1,
						DeprecatedPartialResponseStrategy: PartialResponseStrategy_ABORT,
						PartialResponseStrategy:           PartialResponseStrategy_ABORT,
					},
				},
			},
		},
	} {
		if ok := t.Run(tcase.name, func(t *testing.T) {
			jsonInput, err := json.Marshal(tcase.input)
			testutil.Ok(t, err)

			proto := &RuleGroups{}
			err = json.Unmarshal(jsonInput, proto)
			if tcase.expectedErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tcase.expectedErr.Error(), err.Error())
				return
			}
			testutil.Ok(t, err)
			testutil.Equals(t, tcase.expectedProto.String(), proto.String())

			jsonProto, err := json.Marshal(proto)
			testutil.Ok(t, err)
			if tcase.expectedJSONOutput != "" {
				testutil.Equals(t, tcase.expectedJSONOutput, string(jsonProto))
				return
			}
			testutil.Equals(t, jsonInput, jsonProto)
		}); !ok {
			return
		}
	}
}
