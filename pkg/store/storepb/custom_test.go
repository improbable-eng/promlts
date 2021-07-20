// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package storepb

import (
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/thanos-io/thanos/pkg/store/labelpb"
	"github.com/thanos-io/thanos/pkg/testutil"
)

type sample struct {
	t int64
	v float64
}

type listSeriesSet struct {
	series []Series
	idx    int
}

func newSeries(tb testing.TB, lset labels.Labels, smplChunks [][]sample) Series {
	s := Series{
		Labels: labelpb.ZLabelsFromPromLabels(lset),
	}

	for _, smpls := range smplChunks {
		c := chunkenc.NewXORChunk()
		a, err := c.Appender()
		testutil.Ok(tb, err)

		for _, smpl := range smpls {
			a.Append(smpl.t, smpl.v)
		}

		ch := AggrChunk{
			MinTime: smpls[0].t,
			MaxTime: smpls[len(smpls)-1].t,
			Raw:     &Chunk{Type: Chunk_XOR, Data: c.Bytes()},
		}

		s.Chunks = append(s.Chunks, ch)
	}
	return s
}

func newListSeriesSet(tb testing.TB, raw []rawSeries) *listSeriesSet {
	var series []Series
	for _, s := range raw {
		series = append(series, newSeries(tb, s.lset, s.chunks))
	}
	return &listSeriesSet{
		series: series,
		idx:    -1,
	}
}

func (s *listSeriesSet) Next() bool {
	s.idx++
	return s.idx < len(s.series)
}

func (s *listSeriesSet) At() (labels.Labels, []AggrChunk) {
	if s.idx < 0 || s.idx >= len(s.series) {
		return nil, nil
	}

	return s.series[s.idx].PromLabels(), s.series[s.idx].Chunks
}

func (s *listSeriesSet) Err() error { return nil }

type errSeriesSet struct{ err error }

func (errSeriesSet) Next() bool { return false }

func (errSeriesSet) At() (labels.Labels, []AggrChunk) { return nil, nil }

func (e errSeriesSet) Err() error { return e.err }

func TestMergeSeriesSets(t *testing.T) {
	for _, tcase := range []struct {
		desc     string
		in       [][]rawSeries
		expected []rawSeries
	}{
		{
			desc:     "nils",
			in:       nil,
			expected: nil,
		},
		{
			desc: "single seriesSet, distinct series",
			in: [][]rawSeries{{{
				lset:   labels.FromStrings("a", "a"),
				chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
			}, {
				lset:   labels.FromStrings("a", "c"),
				chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
			}}},

			expected: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
				}, {
					lset:   labels.FromStrings("a", "c"),
					chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
				},
			},
		},
		{
			desc: "two seriesSets, distinct series",
			in: [][]rawSeries{{{
				lset:   labels.FromStrings("a", "a"),
				chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
			}}, {{
				lset:   labels.FromStrings("a", "c"),
				chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
			}}},

			expected: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
				}, {
					lset:   labels.FromStrings("a", "c"),
					chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
				},
			},
		},
		{
			desc: "two seriesSets, {a=c} series to merge, sorted",
			in: [][]rawSeries{
				{
					{
						lset:   labels.FromStrings("a", "a"),
						chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
					},
					{
						lset:   labels.FromStrings("a", "c"),
						chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
					},
				},
				{
					{
						lset:   labels.FromStrings("a", "c"),
						chunks: [][]sample{{{7, 1}, {8, 2}}, {{9, 3}, {10, 4}, {11, 4444}}}, // Last sample overlaps, merge ignores that.
					},
				},
			},

			expected: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
				}, {
					lset:   labels.FromStrings("a", "c"),
					chunks: [][]sample{{{7, 1}, {8, 2}}, {{9, 3}, {10, 4}, {11, 4444}}, {{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
				},
			},
		},
		{
			desc: "single seriesSets, {a=c} series to merge, sorted",
			in: [][]rawSeries{
				{
					{
						lset:   labels.FromStrings("a", "a"),
						chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
					},
					{
						lset:   labels.FromStrings("a", "c"),
						chunks: [][]sample{{{7, 1}, {8, 2}}, {{9, 3}, {10, 4}, {11, 4444}}},
					},
					{
						lset:   labels.FromStrings("a", "c"),
						chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
					},
				},
			},

			expected: []rawSeries{
				{
					lset:   labels.FromStrings("a", "a"),
					chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
				}, {
					lset:   labels.FromStrings("a", "c"),
					chunks: [][]sample{{{7, 1}, {8, 2}}, {{9, 3}, {10, 4}, {11, 4444}}, {{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
				},
			},
		},
		{
			desc: "four seriesSets, {a=c} series to merge AND deduplicate exactly the same chunks",
			in: [][]rawSeries{
				{
					{
						lset:   labels.FromStrings("a", "a"),
						chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
					},
					{
						lset: labels.FromStrings("a", "c"),
						chunks: [][]sample{
							{{11, 11}, {12, 12}, {13, 13}, {14, 14}},
							{{15, 15}, {16, 16}, {17, 17}, {18, 18}},
						},
					},
					{
						lset: labels.FromStrings("a", "c"),
						chunks: [][]sample{
							{{20, 20}, {21, 21}, {22, 22}, {24, 24}},
						},
					},
				},
				{
					{
						lset: labels.FromStrings("a", "c"),
						chunks: [][]sample{
							{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
							{{11, 11}, {12, 12}, {13, 13}, {14, 14}}, // Same chunk as in set 1.
						},
					},
					{
						lset:   labels.FromStrings("a", "d"),
						chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
					},
				},
				{
					{
						lset: labels.FromStrings("a", "c"),
						chunks: [][]sample{
							{{11, 11}, {12, 12}, {13, 13}, {14, 14}}, // Same chunk as in set 1.
							{{20, 20}, {21, 21}, {22, 23}, {24, 24}}, // Almost same chunk as in set 1 (one value is different).
						},
					},
				},
				{
					{
						lset: labels.FromStrings("a", "c"),
						chunks: [][]sample{
							{{11, 11}, {12, 12}, {14, 14}},           // Almost same chunk as in set 1 (one sample is missing).
							{{20, 20}, {21, 21}, {22, 22}, {24, 24}}, // Same chunk as in set 1.
						},
					},
				},
			},

			expected: []rawSeries{
				{
					lset:   labels.Labels{labels.Label{Name: "a", Value: "a"}},
					chunks: [][]sample{{{t: 1, v: 1}, {t: 2, v: 2}}, {{t: 3, v: 3}, {t: 4, v: 4}}},
				}, {
					lset: labels.Labels{labels.Label{Name: "a", Value: "c"}},
					chunks: [][]sample{
						{{t: 1, v: 1}, {t: 2, v: 2}, {t: 3, v: 3}, {t: 4, v: 4}},
						{{t: 11, v: 11}, {t: 12, v: 12}, {t: 13, v: 13}, {t: 14, v: 14}},
						{{t: 11, v: 11}, {t: 12, v: 12}, {t: 14, v: 14}},
						{{t: 15, v: 15}, {t: 16, v: 16}, {t: 17, v: 17}, {t: 18, v: 18}},
						{{t: 20, v: 20}, {t: 21, v: 21}, {t: 22, v: 22}, {t: 24, v: 24}},
						{{t: 20, v: 20}, {t: 21, v: 21}, {t: 22, v: 23}, {t: 24, v: 24}},
					},
				}, {
					lset:   labels.Labels{labels.Label{Name: "a", Value: "d"}},
					chunks: [][]sample{{{t: 11, v: 1}, {t: 12, v: 2}}, {{t: 13, v: 3}, {t: 14, v: 4}}},
				},
			},
		},
		{
			desc: "four seriesSets, {a=c} series to merge, unsorted chunks, so dedup is expected to not be fully done",
			in: [][]rawSeries{
				{
					{
						lset: labels.FromStrings("a", "c"),
						chunks: [][]sample{
							{{20, 20}, {21, 21}, {22, 22}, {24, 24}},
						},
					},
					{
						lset: labels.FromStrings("a", "c"),
						chunks: [][]sample{
							{{11, 11}, {12, 12}, {13, 13}, {14, 14}},
							{{15, 15}, {16, 16}, {17, 17}, {18, 18}},
						},
					},
				},
				{
					{
						lset: labels.FromStrings("a", "c"),
						chunks: [][]sample{
							{{11, 11}, {12, 12}, {13, 13}, {14, 14}}, // Same chunk as in set 1.
							{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
						},
					},
				},
				{
					{
						lset: labels.FromStrings("a", "c"),
						chunks: [][]sample{
							{{20, 20}, {21, 21}, {22, 23}, {24, 24}}, // Almost same chunk as in set 1 (one value is different).
							{{11, 11}, {12, 12}, {13, 13}, {14, 14}}, // Same chunk as in set 1.
						},
					},
				},
			},

			expected: []rawSeries{
				{
					lset: labels.Labels{labels.Label{Name: "a", Value: "c"}},
					chunks: [][]sample{
						{{t: 11, v: 11}, {t: 12, v: 12}, {t: 13, v: 13}, {t: 14, v: 14}},
						{{t: 1, v: 1}, {t: 2, v: 2}, {t: 3, v: 3}, {t: 4, v: 4}},
						{{t: 20, v: 20}, {t: 21, v: 21}, {t: 22, v: 22}, {t: 24, v: 24}},
						{{t: 11, v: 11}, {t: 12, v: 12}, {t: 13, v: 13}, {t: 14, v: 14}},
						{{t: 15, v: 15}, {t: 16, v: 16}, {t: 17, v: 17}, {t: 18, v: 18}},
						{{t: 20, v: 20}, {t: 21, v: 21}, {t: 22, v: 23}, {t: 24, v: 24}},
						{{t: 11, v: 11}, {t: 12, v: 12}, {t: 13, v: 13}, {t: 14, v: 14}},
					},
				},
			},
		},
	} {
		t.Run(tcase.desc, func(t *testing.T) {
			ensureEachSetIsSorted(t, tcase.in) // Make sure input makes sense. We expect input series from each set to be sorted.

			var input []SeriesSet
			for _, iss := range tcase.in {
				input = append(input, newListSeriesSet(t, iss))
			}
			testutil.Equals(t, tcase.expected, expandSeriesSet(t, MergeSeriesSets(input...)))
		})
	}
}

func ensureEachSetIsSorted(t *testing.T, ser [][]rawSeries) {
	t.Helper()

	cpy := make([][]labels.Labels, len(ser))
	cpy2 := make([][]labels.Labels, len(ser))
	for i, s := range ser {
		cpy[i] = make([]labels.Labels, len(s))
		cpy2[i] = make([]labels.Labels, len(s))
		for j, l := range s {
			cpy[i][j] = l.lset
			cpy2[i][j] = l.lset
		}
	}

	// Sort cpy and show diff.
	for _, s := range cpy {
		sort.Slice(s, func(i, j int) bool {
			return labels.Compare(s[i], s[j]) < 0
		})
	}
	testutil.Equals(t, cpy2, cpy)
}

func TestMergeSeriesSetError(t *testing.T) {
	var input []SeriesSet
	for _, iss := range [][]rawSeries{{{
		lset:   labels.FromStrings("a", "a"),
		chunks: [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}},
	}}, {{
		lset:   labels.FromStrings("a", "c"),
		chunks: [][]sample{{{11, 1}, {12, 2}}, {{13, 3}, {14, 4}}},
	}}} {
		input = append(input, newListSeriesSet(t, iss))
	}
	expectedErr := errors.New("test error")
	ss := MergeSeriesSets(append(input, errSeriesSet{err: expectedErr})...)
	testutil.Equals(t, expectedErr, ss.Err())
}

type rawSeries struct {
	lset   labels.Labels
	chunks [][]sample
}

func expandSeriesSet(t *testing.T, gotSS SeriesSet) (ret []rawSeries) {
	for gotSS.Next() {
		lset, chks := gotSS.At()

		r := rawSeries{lset: lset, chunks: make([][]sample, len(chks))}
		for i, chk := range chks {
			c, err := chunkenc.FromData(chunkenc.EncXOR, chk.Raw.Data)
			testutil.Ok(t, err)

			iter := c.Iterator(nil)
			for iter.Next() {
				t, v := iter.At()
				r.chunks[i] = append(r.chunks[i], sample{t: t, v: v})
			}
			testutil.Ok(t, iter.Err())
		}
		ret = append(ret, r)
	}
	testutil.Ok(t, gotSS.Err())
	return ret
}

// Test the cost of merging series sets for different number of merged sets and their size.
func BenchmarkMergedSeriesSet(b *testing.B) {
	b.Run("overlapping chunks", func(b *testing.B) {
		benchmarkMergedSeriesSet(testutil.NewTB(b), true)
	})
	b.Run("non-overlapping chunks", func(b *testing.B) {
		benchmarkMergedSeriesSet(testutil.NewTB(b), false)
	})
}

func TestMergedSeriesSet_Labels(t *testing.T) {
	t.Run("overlapping chunks", func(t *testing.T) {
		benchmarkMergedSeriesSet(testutil.NewTB(t), true)
	})
	t.Run("non-overlapping chunks", func(t *testing.T) {
		benchmarkMergedSeriesSet(testutil.NewTB(t), false)
	})
}

func benchmarkMergedSeriesSet(b testutil.TB, overlappingChunks bool) {
	var sel func(sets []SeriesSet) SeriesSet
	sel = func(sets []SeriesSet) SeriesSet {
		if len(sets) == 0 {
			return EmptySeriesSet()
		}
		if len(sets) == 1 {
			return sets[0]
		}
		l := len(sets) / 2
		return newMergedSeriesSet(sel(sets[:l]), sel(sets[l:]))
	}

	chunks := [][]sample{{{1, 1}, {2, 2}}, {{3, 3}, {4, 4}}}
	for _, k := range []int{
		100,
		1000,
		10000,
		20000,
	} {
		for _, j := range []int{1, 2, 4, 8, 16, 32} {
			b.Run(fmt.Sprintf("series=%d,blocks=%d", k, j), func(b testutil.TB) {
				lbls, err := labels.ReadLabels(filepath.Join("../../testutil/testdata", "20kseries.json"), k)
				testutil.Ok(b, err)

				sort.Sort(labels.Slice(lbls))

				blocks := make([][]rawSeries, j)
				for _, l := range lbls {
					for j := range blocks {
						if overlappingChunks {
							blocks[j] = append(blocks[j], rawSeries{lset: l, chunks: chunks})
							continue
						}
						blocks[j] = append(blocks[j], rawSeries{
							lset: l,
							chunks: [][]sample{
								{{int64(4*j) + 1, 1}, {int64(4*j) + 2, 2}},
								{{int64(4*j) + 3, 3}, {int64(4*j) + 4, 4}},
							},
						})
					}
				}

				b.ResetTimer()

				for i := 0; i < b.N(); i++ {
					var sets []SeriesSet
					for _, s := range blocks {
						sets = append(sets, newListSeriesSet(b, s))
					}
					ms := sel(sets)

					i := 0
					for ms.Next() {
						i++
					}
					testutil.Ok(b, ms.Err())
					testutil.Equals(b, len(lbls), i)
				}
			})
		}
	}
}

func TestMatchersToString_Translate(t *testing.T) {
	for _, c := range []struct {
		ms       []LabelMatcher
		expected string
	}{
		{
			ms: []LabelMatcher{
				{Name: "__name__", Type: LabelMatcher_EQ, Value: "up"},
			},
			expected: `{__name__="up"}`,
		},
		{
			ms: []LabelMatcher{
				{Name: "__name__", Type: LabelMatcher_NEQ, Value: "up"},
				{Name: "job", Type: LabelMatcher_EQ, Value: "test"},
			},
			expected: `{__name__!="up", job="test"}`,
		},
		{
			ms: []LabelMatcher{
				{Name: "__name__", Type: LabelMatcher_EQ, Value: "up"},
				{Name: "job", Type: LabelMatcher_RE, Value: "test"},
			},
			expected: `{__name__="up", job=~"test"}`,
		},
		{
			ms: []LabelMatcher{
				{Name: "job", Type: LabelMatcher_NRE, Value: "test"},
			},
			expected: `{job!~"test"}`,
		},
		{
			ms: []LabelMatcher{
				{Name: "__name__", Type: LabelMatcher_EQ, Value: "up"},
				{Name: "__name__", Type: LabelMatcher_NEQ, Value: "up"},
			},
			// We cannot use up{__name__!="up"} in this case.
			expected: `{__name__="up", __name__!="up"}`,
		},
	} {
		t.Run(c.expected, func(t *testing.T) {
			testutil.Equals(t, c.expected, MatchersToString(c.ms...))

			promMs, err := MatchersToPromMatchers(c.ms...)
			testutil.Ok(t, err)

			testutil.Equals(t, c.expected, PromMatchersToString(promMs...))

			ms, err := PromMatchersToMatchers(promMs...)
			testutil.Ok(t, err)

			testutil.Equals(t, c.ms, ms)
			testutil.Equals(t, c.expected, MatchersToString(ms...))

			// Is this parsable?
			promMsParsed, err := parser.ParseMetricSelector(c.expected)
			testutil.Ok(t, err)
			testutil.Equals(t, promMs, promMsParsed)
		})

	}
}
