// Copyright (c) The Thanos Authors.
// Licensed under the Apache License 2.0.

package downsample

import (
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"go.uber.org/goleak"

	"github.com/thanos-io/thanos/pkg/block"
	"github.com/thanos-io/thanos/pkg/block/metadata"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestDownsampleCounterBoundaryReset(t *testing.T) {
	toAggrChunks := func(t *testing.T, it chunks.Iterator) (res []*AggrChunk, err error) {
		for it.Next() {
			achk, ok := it.At().Chunk.(*AggrChunk)
			testutil.Assert(t, ok, "expected *AggrChunk")
			res = append(res, achk)
		}

		return res, it.Err()
	}

	counterSamples := func(t *testing.T, achks []*AggrChunk) (res []sample) {
		for _, achk := range achks {
			chk, err := achk.Get(AggrCounter)
			testutil.Ok(t, err)

			iter := chk.Iterator(nil)
			for iter.Next() {
				t, v := iter.At()
				res = append(res, sample{t, v})
			}
		}
		return
	}

	counterIterate := func(t *testing.T, achks []*AggrChunk) (res []sample) {
		var iters []chunkenc.Iterator
		for _, achk := range achks {
			chk, err := achk.Get(AggrCounter)
			testutil.Ok(t, err)
			iters = append(iters, chk.Iterator(nil))
		}

		citer := NewApplyCounterResetsIterator(iters...)
		for citer.Next() {
			t, v := citer.At()
			res = append(res, sample{t: t, v: v})
		}
		return
	}

	type test struct {
		raw                   []chunks.Meta
		rawAggrResolution     int64
		expectedRawAggrChunks int
		rawCounterSamples     []sample
		rawCounterIterate     []sample
		aggrAggrResolution    int64
		aggrChunks            int
		aggrCounterSamples    []sample
		aggrCounterIterate    []sample
	}

	tests := []test{
		{
			// In this test case, counter resets occur at the
			// boundaries between the t=49,t=99 and t=99,t=149
			// windows, and the values in the t=49, t=99, and
			// t=149 windows are high enough that the resets
			// will only be accounted for if the first raw value
			// of a chunk is maintained during aggregation.
			// See #1568 for more details.
			raw: []chunks.Meta{
				tsdbutil.ChunkFromSamples([]tsdbutil.Sample{
					sample{10, 1}, sample{20, 3}, sample{30, 5},
					sample{50, 1}, sample{60, 8}, sample{70, 10},
					sample{120, 1}, sample{130, 18}, sample{140, 20},
					sample{160, 21}, sample{170, 38}, sample{180, 40},
				}),
			},
			rawAggrResolution:     50,
			expectedRawAggrChunks: 4,
			rawCounterSamples: []sample{
				{t: 10, v: 1}, {t: 30, v: 5}, {t: 30, v: 5},
				{t: 50, v: 1}, {t: 70, v: 10}, {t: 70, v: 10},
				{t: 120, v: 1}, {t: 140, v: 20}, {t: 140, v: 20},
				{t: 160, v: 21}, {t: 180, v: 40}, {t: 180, v: 40},
			},
			rawCounterIterate: []sample{
				{t: 10, v: 1}, {t: 30, v: 5},
				{t: 50, v: 6}, {t: 70, v: 15},
				{t: 120, v: 16}, {t: 140, v: 35},
				{t: 160, v: 36}, {t: 180, v: 55},
			},
			aggrAggrResolution: 2 * 50,
			aggrChunks:         2,
			aggrCounterSamples: []sample{
				{t: 10, v: 1}, {t: 70, v: 15}, {t: 70, v: 10},
				{t: 120, v: 1}, {t: 180, v: 40}, {t: 180, v: 40},
			},
			aggrCounterIterate: []sample{
				{t: 10, v: 1}, {t: 70, v: 15},
				{t: 120, v: 16}, {t: 180, v: 55},
			},
		},
	}

	doTest := func(t *testing.T, test *test) {
		var (
			buf     []sample
			reuseIt chunkenc.Iterator
		)
		cmIt := newDownsampleRawIterator(test.raw, &buf, reuseIt, 1, test.rawAggrResolution)
		rawAggrChunks, err := toAggrChunks(t, cmIt)
		testutil.Ok(t, err)
		testutil.Equals(t, test.expectedRawAggrChunks, len(rawAggrChunks))
		testutil.Equals(t, test.rawCounterSamples, counterSamples(t, rawAggrChunks))
		testutil.Equals(t, test.rawCounterIterate, counterIterate(t, rawAggrChunks))

		acmIt := newDownsampleAggrIterator(rawAggrChunks, &buf, reuseIt, len(rawAggrChunks)/test.aggrChunks, test.aggrAggrResolution)

		aggrAggrChunks, err := toAggrChunks(t, acmIt)
		testutil.Ok(t, err)
		testutil.Equals(t, test.aggrChunks, len(aggrAggrChunks))
		testutil.Equals(t, test.aggrCounterSamples, counterSamples(t, aggrAggrChunks))
		testutil.Equals(t, test.aggrCounterIterate, counterIterate(t, aggrAggrChunks))
	}

	doTest(t, &tests[0])
}

func TestExpandChunkIterator(t *testing.T) {
	// Validate that expanding the chunk iterator filters out-of-order samples
	// and staleness markers.
	// Same timestamps are okay since we use them for counter markers.
	var res []sample
	testutil.Ok(t,
		expandChunkIterator(
			newSampleIterator([]sample{
				{100, 1}, {200, 2}, {200, 3}, {201, 4}, {200, 5},
				{300, 6}, {400, math.Float64frombits(value.StaleNaN)}, {500, 5},
			}), &res,
		),
	)

	testutil.Equals(t, []sample{{100, 1}, {200, 2}, {200, 3}, {201, 4}, {300, 6}, {500, 5}}, res)
}

var (
	// Decoded excerpt of pkg/query/testdata/issue2401-seriesresponses.json without overlaps (downsampling works directly on blocks).
	realisticChkDataWithStaleMarker = [][]sample{
		{
			{t: 1587690005791, v: 461968}, {t: 1587690020791, v: 462151}, {t: 1587690035797, v: 462336}, {t: 1587690050791, v: 462650}, {t: 1587690065791, v: 462813}, {t: 1587690080791, v: 462987}, {t: 1587690095791, v: 463095}, {t: 1587690110791, v: 463247}, {t: 1587690125791, v: 463440}, {t: 1587690140791, v: 463642},
			{t: 1587690155791, v: 463811}, {t: 1587690170791, v: 464027}, {t: 1587690185791, v: 464308}, {t: 1587690200791, v: 464514}, {t: 1587690215791, v: 464798}, {t: 1587690230791, v: 465018}, {t: 1587690245791, v: 465215}, {t: 1587690260813, v: 465431}, {t: 1587690275791, v: 465651}, {t: 1587690290791, v: 465870},
			{t: 1587690305791, v: 466070}, {t: 1587690320792, v: 466248}, {t: 1587690335791, v: 466506}, {t: 1587690350791, v: 466766}, {t: 1587690365791, v: 466970}, {t: 1587690380791, v: 467123}, {t: 1587690395791, v: 467265}, {t: 1587690410791, v: 467383}, {t: 1587690425791, v: 467629}, {t: 1587690440791, v: 467931},
			{t: 1587690455791, v: 468097}, {t: 1587690470791, v: 468281}, {t: 1587690485791, v: 468477}, {t: 1587690500791, v: 468649}, {t: 1587690515791, v: 468867}, {t: 1587690530791, v: 469150}, {t: 1587690545791, v: 469268}, {t: 1587690560791, v: 469488}, {t: 1587690575791, v: 469742}, {t: 1587690590791, v: 469951},
			{t: 1587690605791, v: 470131}, {t: 1587690620791, v: 470337}, {t: 1587690635791, v: 470631}, {t: 1587690650791, v: 470832}, {t: 1587690665791, v: 471077}, {t: 1587690680791, v: 471311}, {t: 1587690695791, v: 471473}, {t: 1587690710791, v: 471728}, {t: 1587690725791, v: 472002}, {t: 1587690740791, v: 472158},
			{t: 1587690755791, v: 472329}, {t: 1587690770791, v: 472722}, {t: 1587690785791, v: 472925}, {t: 1587690800791, v: 473220}, {t: 1587690815791, v: 473460}, {t: 1587690830791, v: 473748}, {t: 1587690845791, v: 473968}, {t: 1587690860791, v: 474261}, {t: 1587690875791, v: 474418}, {t: 1587690890791, v: 474726},
			{t: 1587690905791, v: 474913}, {t: 1587690920791, v: 475031}, {t: 1587690935791, v: 475284}, {t: 1587690950791, v: 475563}, {t: 1587690965791, v: 475762}, {t: 1587690980791, v: 475945}, {t: 1587690995791, v: 476302}, {t: 1587691010791, v: 476501}, {t: 1587691025791, v: 476849}, {t: 1587691040800, v: 477020},
			{t: 1587691055791, v: 477280}, {t: 1587691070791, v: 477549}, {t: 1587691085791, v: 477758}, {t: 1587691100817, v: 477960}, {t: 1587691115791, v: 478261}, {t: 1587691130791, v: 478559}, {t: 1587691145791, v: 478704}, {t: 1587691160804, v: 478950}, {t: 1587691175791, v: 479173}, {t: 1587691190791, v: 479368},
			{t: 1587691205791, v: 479625}, {t: 1587691220805, v: 479866}, {t: 1587691235791, v: 480008}, {t: 1587691250791, v: 480155}, {t: 1587691265791, v: 480472}, {t: 1587691280811, v: 480598}, {t: 1587691295791, v: 480771}, {t: 1587691310791, v: 480996}, {t: 1587691325791, v: 481200}, {t: 1587691340803, v: 481381},
			{t: 1587691355791, v: 481584}, {t: 1587691370791, v: 481759}, {t: 1587691385791, v: 482003}, {t: 1587691400803, v: 482189}, {t: 1587691415791, v: 482457}, {t: 1587691430791, v: 482623}, {t: 1587691445791, v: 482768}, {t: 1587691460804, v: 483036}, {t: 1587691475791, v: 483322}, {t: 1587691490791, v: 483566},
			{t: 1587691505791, v: 483709}, {t: 1587691520807, v: 483838}, {t: 1587691535791, v: 484091}, {t: 1587691550791, v: 484236}, {t: 1587691565791, v: 484454}, {t: 1587691580816, v: 484710}, {t: 1587691595791, v: 484978}, {t: 1587691610791, v: 485271}, {t: 1587691625791, v: 485476}, {t: 1587691640792, v: 485640},
			{t: 1587691655791, v: 485921}, {t: 1587691670791, v: 486201}, {t: 1587691685791, v: 486555}, {t: 1587691700791, v: 486691}, {t: 1587691715791, v: 486831}, {t: 1587691730791, v: 487033}, {t: 1587691745791, v: 487268}, {t: 1587691760803, v: 487370}, {t: 1587691775791, v: 487571}, {t: 1587691790791, v: 487787},
		},
		{
			{t: 1587691805791, v: 488036}, {t: 1587691820791, v: 488241}, {t: 1587691835791, v: 488411}, {t: 1587691850791, v: 488625}, {t: 1587691865791, v: 488868}, {t: 1587691880791, v: 489005}, {t: 1587691895791, v: 489237}, {t: 1587691910791, v: 489545}, {t: 1587691925791, v: 489750}, {t: 1587691940791, v: 489899},
			{t: 1587691955791, v: 490048}, {t: 1587691970791, v: 490364}, {t: 1587691985791, v: 490485}, {t: 1587692000791, v: 490722}, {t: 1587692015791, v: 490866}, {t: 1587692030791, v: 491025}, {t: 1587692045791, v: 491286}, {t: 1587692060816, v: 491543}, {t: 1587692075791, v: 491787}, {t: 1587692090791, v: 492065},
			{t: 1587692105791, v: 492223}, {t: 1587692120816, v: 492501}, {t: 1587692135791, v: 492767}, {t: 1587692150791, v: 492955}, {t: 1587692165791, v: 493194}, {t: 1587692180792, v: 493402}, {t: 1587692195791, v: 493647}, {t: 1587692210791, v: 493897}, {t: 1587692225791, v: 494117}, {t: 1587692240805, v: 494356},
			{t: 1587692255791, v: 494620}, {t: 1587692270791, v: 494762}, {t: 1587692285791, v: 495001}, {t: 1587692300805, v: 495222}, {t: 1587692315791, v: 495393}, {t: 1587692330791, v: 495662}, {t: 1587692345791, v: 495875}, {t: 1587692360801, v: 496082}, {t: 1587692375791, v: 496196}, {t: 1587692390791, v: 496245},
			{t: 1587692405791, v: 496295}, {t: 1587692420791, v: 496365}, {t: 1587692435791, v: 496401}, {t: 1587692450791, v: 496452}, {t: 1587692465791, v: 496491}, {t: 1587692480791, v: 496544}, {t: 1587692495791, v: math.Float64frombits(value.StaleNaN)}, {t: 1587692555791, v: 75}, {t: 1587692570791, v: 308}, {t: 1587692585791, v: 508},
			{t: 1587692600791, v: 701}, {t: 1587692615791, v: 985}, {t: 1587692630791, v: 1153}, {t: 1587692645791, v: 1365}, {t: 1587692660791, v: 1612}, {t: 1587692675803, v: 1922}, {t: 1587692690791, v: 2103}, {t: 1587692705791, v: 2261}, {t: 1587692720791, v: 2469}, {t: 1587692735805, v: 2625},
			{t: 1587692750791, v: 2801}, {t: 1587692765791, v: 2955}, {t: 1587692780791, v: 3187}, {t: 1587692795806, v: 3428}, {t: 1587692810791, v: 3657}, {t: 1587692825791, v: 3810}, {t: 1587692840791, v: 3968}, {t: 1587692855791, v: 4195}, {t: 1587692870791, v: 4414}, {t: 1587692885791, v: 4646},
			{t: 1587692900791, v: 4689}, {t: 1587692915791, v: 4847}, {t: 1587692930791, v: 5105}, {t: 1587692945791, v: 5309}, {t: 1587692960791, v: 5521}, {t: 1587692975791, v: 5695}, {t: 1587692990810, v: 6010}, {t: 1587693005791, v: 6210}, {t: 1587693020791, v: 6394}, {t: 1587693035791, v: 6597},
			{t: 1587693050791, v: 6872}, {t: 1587693065791, v: 7098}, {t: 1587693080791, v: 7329}, {t: 1587693095791, v: 7470}, {t: 1587693110791, v: 7634}, {t: 1587693125821, v: 7830}, {t: 1587693140791, v: 8034}, {t: 1587693155791, v: 8209}, {t: 1587693170791, v: 8499}, {t: 1587693185791, v: 8688},
			{t: 1587693200791, v: 8893}, {t: 1587693215791, v: 9052}, {t: 1587693230791, v: 9379}, {t: 1587693245791, v: 9544}, {t: 1587693260791, v: 9763}, {t: 1587693275791, v: 9974}, {t: 1587693290791, v: 10242}, {t: 1587693305791, v: 10464}, {t: 1587693320803, v: 10716}, {t: 1587693335791, v: 10975},
			{t: 1587693350791, v: 11232}, {t: 1587693365791, v: 11459}, {t: 1587693380791, v: 11778}, {t: 1587693395804, v: 12007}, {t: 1587693410791, v: 12206}, {t: 1587693425791, v: 12450}, {t: 1587693440791, v: 12693}, {t: 1587693455791, v: 12908}, {t: 1587693470791, v: 13158}, {t: 1587693485791, v: 13427},
			{t: 1587693500791, v: 13603}, {t: 1587693515791, v: 13927}, {t: 1587693530816, v: 14122}, {t: 1587693545791, v: 14327}, {t: 1587693560791, v: 14579}, {t: 1587693575791, v: 14759}, {t: 1587693590791, v: 14956},
		},
	}
	realisticChkDataWithCounterResetRes5m = []map[AggrType][]sample{
		{
			AggrCount:   {{t: 1587690299999, v: 20}, {t: 1587690599999, v: 20}, {t: 1587690899999, v: 20}, {t: 1587691199999, v: 20}, {t: 1587691499999, v: 20}, {t: 1587691799999, v: 20}, {t: 1587692099999, v: 20}, {t: 1587692399999, v: 20}, {t: 1587692699999, v: 16}, {t: 1587692999999, v: 20}, {t: 1587693299999, v: 20}, {t: 1587693590791, v: 20}},
			AggrSum:     {{t: 1587690299999, v: 9.276972e+06}, {t: 1587690599999, v: 9.359861e+06}, {t: 1587690899999, v: 9.447457e+06}, {t: 1587691199999, v: 9.542732e+06}, {t: 1587691499999, v: 9.630379e+06}, {t: 1587691799999, v: 9.715631e+06}, {t: 1587692099999, v: 9.799808e+06}, {t: 1587692399999, v: 9.888117e+06}, {t: 1587692699999, v: 2.98928e+06}, {t: 1587692999999, v: 81592}, {t: 1587693299999, v: 163711}, {t: 1587693590791, v: 255746}},
			AggrMin:     {{t: 1587690299999, v: 461968}, {t: 1587690599999, v: 466070}, {t: 1587690899999, v: 470131}, {t: 1587691199999, v: 474913}, {t: 1587691499999, v: 479625}, {t: 1587691799999, v: 483709}, {t: 1587692099999, v: 488036}, {t: 1587692399999, v: 492223}, {t: 1587692699999, v: 75}, {t: 1587692999999, v: 2261}, {t: 1587693299999, v: 6210}, {t: 1587693590791, v: 10464}},
			AggrMax:     {{t: 1587690299999, v: 465870}, {t: 1587690599999, v: 469951}, {t: 1587690899999, v: 474726}, {t: 1587691199999, v: 479368}, {t: 1587691499999, v: 483566}, {t: 1587691799999, v: 487787}, {t: 1587692099999, v: 492065}, {t: 1587692399999, v: 496245}, {t: 1587692699999, v: 496544}, {t: 1587692999999, v: 6010}, {t: 1587693299999, v: 10242}, {t: 1587693590791, v: 14956}},
			AggrCounter: {{t: 1587690005791, v: 461968}, {t: 1587690299999, v: 465870}, {t: 1587690599999, v: 469951}, {t: 1587690899999, v: 474726}, {t: 1587691199999, v: 479368}, {t: 1587691499999, v: 483566}, {t: 1587691799999, v: 487787}, {t: 1587692099999, v: 492065}, {t: 1587692399999, v: 496245}, {t: 1587692699999, v: 498647}, {t: 1587692999999, v: 502554}, {t: 1587693299999, v: 506786}, {t: 1587693590791, v: 511500}, {t: 1587693590791, v: 14956}},
		},
	}
)

func TestDownsample(t *testing.T) {
	type downsampleTestCase struct {
		name string

		// Either inRaw or inAggr should be provided.
		inRaw      [][]sample
		inAggr     []map[AggrType][]sample
		resolution int64

		// Expected output.
		expected                []map[AggrType][]sample
		expectedDownsamplingErr func([]chunks.Meta) error
	}
	for _, tcase := range []*downsampleTestCase{
		{
			name: "single chunk",
			inRaw: [][]sample{
				{{20, 1}, {40, 2}, {60, 3}, {80, 1}, {100, 2}, {101, math.Float64frombits(value.StaleNaN)}, {120, 5}, {180, 10}, {250, 1}},
			},
			resolution: 100,

			expected: []map[AggrType][]sample{
				{
					AggrCount:   {{99, 4}, {199, 3}, {250, 1}},
					AggrSum:     {{99, 7}, {199, 17}, {250, 1}},
					AggrMin:     {{99, 1}, {199, 2}, {250, 1}},
					AggrMax:     {{99, 3}, {199, 10}, {250, 1}},
					AggrCounter: {{20, 1}, {99, 4}, {199, 13}, {250, 14}, {250, 1}},
				},
			},
		},
		{
			name: "three chunks",
			inRaw: [][]sample{
				{{20, 1}, {40, 2}, {60, 3}, {80, 1}, {100, 2}, {101, math.Float64frombits(value.StaleNaN)}, {120, 5}, {180, 10}, {250, 2}},
				{{260, 1}, {300, 10}, {340, 15}, {380, 25}, {420, 35}},
				{{460, math.Float64frombits(value.StaleNaN)}, {500, 10}, {540, 3}},
			},
			resolution: 100,

			expected: []map[AggrType][]sample{
				{
					AggrCount:   {{t: 99, v: 4}, {t: 199, v: 3}, {t: 299, v: 2}, {t: 399, v: 3}, {t: 499, v: 1}, {t: 540, v: 2}},
					AggrSum:     {{t: 99, v: 7}, {t: 199, v: 17}, {t: 299, v: 3}, {t: 399, v: 50}, {t: 499, v: 35}, {t: 540, v: 13}},
					AggrMin:     {{t: 99, v: 1}, {t: 199, v: 2}, {t: 299, v: 1}, {t: 399, v: 10}, {t: 499, v: 35}, {t: 540, v: 3}},
					AggrMax:     {{t: 99, v: 3}, {t: 199, v: 10}, {t: 299, v: 2}, {t: 399, v: 25}, {t: 499, v: 35}, {t: 540, v: 10}},
					AggrCounter: {{t: 20, v: 1}, {t: 99, v: 4}, {t: 199, v: 13}, {t: 299, v: 16}, {t: 399, v: 40}, {t: 499, v: 50}, {t: 540, v: 63}, {t: 540, v: 3}},
				},
			},
		},
		{
			name: "four chunks, two of them overlapping",
			inRaw: [][]sample{
				{{20, 1}, {40, 2}, {60, 3}, {80, 1}, {100, 2}, {101, math.Float64frombits(value.StaleNaN)}, {120, 5}, {180, 10}, {250, 2}},
				{{20, 1}, {40, 2}, {60, 3}, {80, 1}, {100, 2}, {101, math.Float64frombits(value.StaleNaN)}, {120, 5}, {180, 10}, {250, 2}},
				{{260, 1}, {300, 10}, {340, 15}, {380, 25}, {420, 35}},
				{{460, math.Float64frombits(value.StaleNaN)}, {500, 10}, {540, 3}},
			},
			resolution: 100,

			expectedDownsamplingErr: func(chks []chunks.Meta) error {
				return errors.Errorf("found overlapping chunks within series 0. Chunks expected to be ordered by min time and non-overlapping, got: %v", chks)
			},
		},
		{
			name:       "realistic 15s interval raw chunks",
			inRaw:      realisticChkDataWithStaleMarker,
			resolution: ResLevel1, // 5m.

			expected: realisticChkDataWithCounterResetRes5m,
		},
		// Aggregated -> Downsampled Aggregated.
		{
			name: "single aggregated chunks",
			inAggr: []map[AggrType][]sample{
				{
					AggrCount: {{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100}},
					AggrSum:   {{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100}},
					AggrMin:   {{199, 5}, {299, 1}, {399, 10}, {400, -3}, {499, 10}, {699, 0}, {999, 100}},
					AggrMax:   {{199, 5}, {299, 1}, {399, 10}, {400, -3}, {499, 10}, {699, 0}, {999, 100}},
					AggrCounter: {
						{99, 100}, {299, 150}, {499, 210}, {499, 10}, // Chunk 1.
						{599, 20}, {799, 50}, {999, 120}, {999, 50}, // Chunk 2, no reset.
						{1099, 40}, {1199, 80}, {1299, 110}, // Chunk 3, reset.
					},
				},
			},
			resolution: 500,

			expected: []map[AggrType][]sample{
				{
					AggrCount:   {{499, 29}, {999, 100}},
					AggrSum:     {{499, 29}, {999, 100}},
					AggrMin:     {{499, -3}, {999, 0}},
					AggrMax:     {{499, 10}, {999, 100}},
					AggrCounter: {{99, 100}, {499, 210}, {999, 320}, {1299, 430}, {1299, 110}},
				},
			},
		},
		func() *downsampleTestCase {
			downsample500resolutionChunk := []map[AggrType][]sample{
				{
					AggrCount:   {{499, 29}, {999, 100}},
					AggrSum:     {{499, 29}, {999, 100}},
					AggrMin:     {{499, -3}, {999, 0}},
					AggrMax:     {{499, 10}, {999, 100}},
					AggrCounter: {{99, 100}, {499, 210}, {999, 320}, {1299, 430}, {1299, 110}},
				},
			}
			return &downsampleTestCase{
				name:       "downsampling already downsampled to the same resolution aggregated chunks",
				resolution: 500,

				// Should be the output as input.
				inAggr:   downsample500resolutionChunk,
				expected: downsample500resolutionChunk,
			}
		}(),
		{
			name: "two aggregated chunks",
			inAggr: []map[AggrType][]sample{
				{
					AggrCount: {{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100}},
					AggrSum:   {{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100}},
					AggrMin:   {{199, 5}, {299, 1}, {399, 10}, {400, -3}, {499, 10}, {699, 0}, {999, 100}},
					AggrMax:   {{199, 5}, {299, 1}, {399, 10}, {400, -3}, {499, 10}, {699, 0}, {999, 100}},
					AggrCounter: {
						{99, 100}, {299, 150}, {499, 210}, {499, 10}, // Chunk 1.
						{599, 20}, {799, 50}, {999, 120}, {999, 50}, // Chunk 2, no reset.
						{1099, 40}, {1199, 80}, {1299, 110}, // Chunk 3, reset.
					},
				},
				{
					AggrCount: {{1399, 10}, {1400, 3}, {1499, 10}, {1699, 0}, {1999, 100}},
					AggrSum:   {{1399, 10}, {1400, 3}, {1499, 10}, {1699, 0}, {1999, 100}},
					AggrMin:   {{1399, 10}, {1400, -3}, {1499, 10}, {1699, 0}, {1999, 100}},
					AggrMax:   {{1399, 10}, {1400, -3}, {1499, 10}, {1699, 0}, {1999, 100}},
					AggrCounter: {
						{1499, 210}, {1499, 10}, // Chunk 1.
						{1599, 20}, {1799, 50}, {1999, 120}, {1999, 50}, // Chunk 2, no reset.
						{2099, 40}, {2199, 80}, {2299, 110}, // Chunk 3, reset.
					},
				},
			},
			resolution: 500,

			expected: []map[AggrType][]sample{
				{
					AggrCount:   {{t: 499, v: 29}, {t: 999, v: 100}, {t: 1499, v: 23}, {t: 1999, v: 100}},
					AggrSum:     {{t: 499, v: 29}, {t: 999, v: 100}, {t: 1499, v: 23}, {t: 1999, v: 100}},
					AggrMin:     {{t: 499, v: -3}, {t: 999, v: 0}, {t: 1499, v: -3}, {t: 1999, v: 0}},
					AggrMax:     {{t: 499, v: 10}, {t: 999, v: 100}, {t: 1499, v: 10}, {t: 1999, v: 100}},
					AggrCounter: {{t: 99, v: 100}, {t: 499, v: 210}, {t: 999, v: 320}, {t: 1499, v: 530}, {t: 1999, v: 640}, {t: 2299, v: 750}, {t: 2299, v: 110}},
				},
			},
		},
		{
			name: "two aggregated, overlapping chunks",
			inAggr: []map[AggrType][]sample{
				{
					AggrCount: {{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100}},
				},
				{
					AggrCount: {{199, 5}, {299, 1}, {399, 10}, {400, 3}, {499, 10}, {699, 0}, {999, 100}},
				},
			},
			resolution: 500,

			expectedDownsamplingErr: func(chks []chunks.Meta) error {
				return errors.Errorf("found overlapping chunks within series 0. Chunks expected to be ordered by min time and non-overlapping, got: %v", chks)
			},
		},
		{
			name: "realistic ResLevel1 (5m) downsampled chunks with from counter resets",
			inAggr: []map[AggrType][]sample{
				{
					AggrCount:   {{t: 1587690299999, v: 20}, {t: 1587690599999, v: 20}, {t: 1587690899999, v: 20}, {t: 1587691199999, v: 20}, {t: 1587691499999, v: 20}, {t: 1587691799999, v: 20}, {t: 1587692099999, v: 20}, {t: 1587692399999, v: 20}, {t: 1587692699999, v: 16}, {t: 1587692999999, v: 20}, {t: 1587693299999, v: 20}, {t: 1587693590791, v: 20}},
					AggrSum:     {{t: 1587690299999, v: 9.276972e+06}, {t: 1587690599999, v: 9.359861e+06}, {t: 1587690899999, v: 9.447457e+06}, {t: 1587691199999, v: 9.542732e+06}, {t: 1587691499999, v: 9.630379e+06}, {t: 1587691799999, v: 9.715631e+06}, {t: 1587692099999, v: 9.799808e+06}, {t: 1587692399999, v: 9.888117e+06}, {t: 1587692699999, v: 2.98928e+06}, {t: 1587692999999, v: 81592}, {t: 1587693299999, v: 163711}, {t: 1587693590791, v: 255746}},
					AggrMin:     {{t: 1587690299999, v: 461968}, {t: 1587690599999, v: 466070}, {t: 1587690899999, v: 470131}, {t: 1587691199999, v: 474913}, {t: 1587691499999, v: 479625}, {t: 1587691799999, v: 483709}, {t: 1587692099999, v: 488036}, {t: 1587692399999, v: 492223}, {t: 1587692699999, v: 75}, {t: 1587692999999, v: 2261}, {t: 1587693299999, v: 6210}, {t: 1587693590791, v: 10464}},
					AggrMax:     {{t: 1587690299999, v: 465870}, {t: 1587690599999, v: 469951}, {t: 1587690899999, v: 474726}, {t: 1587691199999, v: 479368}, {t: 1587691499999, v: 483566}, {t: 1587691799999, v: 487787}, {t: 1587692099999, v: 492065}, {t: 1587692399999, v: 496245}, {t: 1587692699999, v: 496544}, {t: 1587692999999, v: 6010}, {t: 1587693299999, v: 10242}, {t: 1587693590791, v: 14956}},
					AggrCounter: {{t: 1587690005791, v: 461968}, {t: 1587690299999, v: 465870}, {t: 1587690599999, v: 469951}, {t: 1587690899999, v: 474726}, {t: 1587691199999, v: 479368}, {t: 1587691499999, v: 483566}, {t: 1587691799999, v: 487787}, {t: 1587692099999, v: 492065}, {t: 1587692399999, v: 496245}, {t: 1587692699999, v: 498647}, {t: 1587692999999, v: 502554}, {t: 1587693299999, v: 506786}, {t: 1587693590791, v: 511500}, {t: 1587693590791, v: 14956}},
				},
			},
			resolution: ResLevel2,

			expected: []map[AggrType][]sample{
				{
					AggrCount:   {{t: 1587693590791, v: 236}},
					AggrSum:     {{t: 1587693590791, v: 8.0151286e+07}},
					AggrMin:     {{t: 1587693590791, v: 75}},
					AggrMax:     {{t: 1587693590791, v: 496544}},
					AggrCounter: {{t: 1587690005791, v: 461968}, {t: 1587693590791, v: 511500}, {t: 1587693590791, v: 14956}},
				},
			},
		},
		// TODO(bwplotka): This is not very efficient for further query time, we should produce 2 chunks. Fix it https://github.com/thanos-io/thanos/issues/2542.
		func() *downsampleTestCase {
			d := &downsampleTestCase{
				name:       "downsampling four, 120 sample chunks for 2x resolution should result in two chunks, but results in one.",
				resolution: 2,
				inAggr:     []map[AggrType][]sample{{AggrCounter: {}}, {AggrCounter: {}}, {AggrCounter: {}}, {AggrCounter: {}}},
				expected:   []map[AggrType][]sample{{AggrCounter: {}}},
			}

			for i := int64(0); i < 120; i++ {
				d.inAggr[0][AggrCounter] = append(d.inAggr[0][AggrCounter], sample{t: i, v: float64(i)})
				d.inAggr[1][AggrCounter] = append(d.inAggr[1][AggrCounter], sample{t: 120 + i, v: float64(120 + i)})
				d.inAggr[2][AggrCounter] = append(d.inAggr[2][AggrCounter], sample{t: 240 + i, v: float64(240 + i)})
				d.inAggr[3][AggrCounter] = append(d.inAggr[3][AggrCounter], sample{t: 360 + i, v: float64(360 + i)})
			}

			d.expected[0][AggrCounter] = append(d.expected[0][AggrCounter], sample{t: 0, v: float64(0)})
			for i := int64(0); i < 480; i += 2 {
				d.expected[0][AggrCounter] = append(d.expected[0][AggrCounter], sample{t: 1 + i, v: float64(1 + i)})
			}
			d.expected[0][AggrCounter] = append(d.expected[0][AggrCounter], sample{t: 479, v: 479})

			return d
		}(),
	} {
		t.Run(tcase.name, func(t *testing.T) {
			logger := log.NewLogfmtLogger(os.Stderr)

			dir, err := ioutil.TempDir("", "downsample-raw")
			testutil.Ok(t, err)
			defer func() { testutil.Ok(t, os.RemoveAll(dir)) }()

			// Ideally we would use tsdb.HeadBlock here for less dependency on our own code. However,
			// it cannot accept the counter signal sample with the same timestamp as the previous sample.
			mb := newMemBlock()
			ser := chunksToSeriesIteratable(t, tcase.inRaw, tcase.inAggr)
			mb.addSeries(ser)

			fakeMeta := &metadata.Meta{}
			if len(tcase.inAggr) > 0 {
				fakeMeta.Thanos.Downsample.Resolution = tcase.resolution - 1
			}

			id, err := Downsample(logger, fakeMeta, mb, dir, tcase.resolution)
			if tcase.expectedDownsamplingErr != nil {
				testutil.NotOk(t, err)
				testutil.Equals(t, tcase.expectedDownsamplingErr(ser.chunks).Error(), err.Error())
				return
			}
			testutil.Ok(t, err)

			_, err = metadata.ReadFromDir(filepath.Join(dir, id.String()))
			testutil.Ok(t, err)

			indexr, err := index.NewFileReader(filepath.Join(dir, id.String(), block.IndexFilename))
			testutil.Ok(t, err)
			defer func() { testutil.Ok(t, indexr.Close()) }()

			chunkr, err := chunks.NewDirReader(filepath.Join(dir, id.String(), block.ChunksDirname), NewPool())
			testutil.Ok(t, err)
			defer func() { testutil.Ok(t, chunkr.Close()) }()

			pall, err := indexr.Postings(index.AllPostingsKey())
			testutil.Ok(t, err)

			var series []uint64
			for pall.Next() {
				series = append(series, pall.At())
			}
			testutil.Ok(t, pall.Err())
			testutil.Equals(t, 1, len(series))

			var lset labels.Labels
			var chks []chunks.Meta
			testutil.Ok(t, indexr.Series(series[0], &lset, &chks))
			testutil.Equals(t, labels.FromStrings("__name__", "a"), lset)

			var got []map[AggrType][]sample
			for _, c := range chks {
				chk, err := chunkr.Chunk(c.Ref)
				testutil.Ok(t, err)

				m := map[AggrType][]sample{}
				for _, at := range []AggrType{AggrCount, AggrSum, AggrMin, AggrMax, AggrCounter} {
					c, err := chk.(*AggrChunk).Get(at)
					if err == ErrAggrNotExist {
						continue
					}
					testutil.Ok(t, err)

					buf := m[at]
					testutil.Ok(t, expandChunkIterator(c.Iterator(nil), &buf))
					m[at] = buf
				}
				got = append(got, m)
			}
			testutil.Equals(t, tcase.expected, got)
		})
	}
}

func chunksToSeriesIteratable(t *testing.T, inRaw [][]sample, inAggr []map[AggrType][]sample) *series {
	if len(inRaw) > 0 && len(inAggr) > 0 {
		t.Fatalf("test must not have raw and aggregate input data at once")
	}
	ser := &series{lset: labels.FromStrings("__name__", "a")}

	if len(inRaw) > 0 {
		for _, samples := range inRaw {
			chk := chunkenc.NewXORChunk()
			app, _ := chk.Appender()

			for _, s := range samples {
				app.Append(s.t, s.v)
			}
			ser.chunks = append(ser.chunks, chunks.Meta{
				MinTime: samples[0].t,
				MaxTime: samples[len(samples)-1].t,
				Chunk:   chk,
			})
		}
		return ser
	}

	for _, chk := range inAggr {
		ser.chunks = append(ser.chunks, encodeTestAggrSeries(chk))
	}
	return ser
}
func encodeTestAggrSeries(v map[AggrType][]sample) chunks.Meta {
	b := newAggrChunkBuilder()
	// we cannot use `b.add` as we have separate samples, do it manually, but make sure to
	// calculate overall chunk time ranges.
	for at, d := range v {
		for _, s := range d {
			if s.t < b.mint {
				b.mint = s.t
			}
			if s.t > b.maxt {
				b.maxt = s.t
			}
			b.apps[at].Append(s.t, s.v)
		}
	}
	return b.encode()
}

func TestAverageChunkIterator(t *testing.T) {
	sum := []sample{{100, 30}, {200, 40}, {300, 5}, {400, -10}}
	cnt := []sample{{100, 1}, {200, 5}, {300, 2}, {400, 10}}
	exp := []sample{{100, 30}, {200, 8}, {300, 2.5}, {400, -1}}

	x := NewAverageChunkIterator(newSampleIterator(cnt), newSampleIterator(sum))

	var res []sample
	for x.Next() {
		t, v := x.At()
		res = append(res, sample{t, v})
	}
	testutil.Ok(t, x.Err())
	testutil.Equals(t, exp, res)
}

var (
	realisticChkDataWithCounterResetsAfterCounterSeriesIterating = []sample{
		{t: 1587690005791, v: 461968}, {t: 1587690020791, v: 462151}, {t: 1587690035797, v: 462336}, {t: 1587690050791, v: 462650}, {t: 1587690065791, v: 462813}, {t: 1587690080791, v: 462987}, {t: 1587690095791, v: 463095}, {t: 1587690110791, v: 463247}, {t: 1587690125791, v: 463440}, {t: 1587690140791, v: 463642}, {t: 1587690155791, v: 463811},
		{t: 1587690170791, v: 464027}, {t: 1587690185791, v: 464308}, {t: 1587690200791, v: 464514}, {t: 1587690215791, v: 464798}, {t: 1587690230791, v: 465018}, {t: 1587690245791, v: 465215}, {t: 1587690260813, v: 465431}, {t: 1587690275791, v: 465651}, {t: 1587690290791, v: 465870}, {t: 1587690305791, v: 466070}, {t: 1587690320792, v: 466248},
		{t: 1587690335791, v: 466506}, {t: 1587690350791, v: 466766}, {t: 1587690365791, v: 466970}, {t: 1587690380791, v: 467123}, {t: 1587690395791, v: 467265}, {t: 1587690410791, v: 467383}, {t: 1587690425791, v: 467629}, {t: 1587690440791, v: 467931}, {t: 1587690455791, v: 468097}, {t: 1587690470791, v: 468281}, {t: 1587690485791, v: 468477},
		{t: 1587690500791, v: 468649}, {t: 1587690515791, v: 468867}, {t: 1587690530791, v: 469150}, {t: 1587690545791, v: 469268}, {t: 1587690560791, v: 469488}, {t: 1587690575791, v: 469742}, {t: 1587690590791, v: 469951}, {t: 1587690605791, v: 470131}, {t: 1587690620791, v: 470337}, {t: 1587690635791, v: 470631}, {t: 1587690650791, v: 470832},
		{t: 1587690665791, v: 471077}, {t: 1587690680791, v: 471311}, {t: 1587690695791, v: 471473}, {t: 1587690710791, v: 471728}, {t: 1587690725791, v: 472002}, {t: 1587690740791, v: 472158}, {t: 1587690755791, v: 472329}, {t: 1587690770791, v: 472722}, {t: 1587690785791, v: 472925}, {t: 1587690800791, v: 473220}, {t: 1587690815791, v: 473460},
		{t: 1587690830791, v: 473748}, {t: 1587690845791, v: 473968}, {t: 1587690860791, v: 474261}, {t: 1587690875791, v: 474418}, {t: 1587690890791, v: 474726}, {t: 1587690905791, v: 474913}, {t: 1587690920791, v: 475031}, {t: 1587690935791, v: 475284}, {t: 1587690950791, v: 475563}, {t: 1587690965791, v: 475762}, {t: 1587690980791, v: 475945},
		{t: 1587690995791, v: 476302}, {t: 1587691010791, v: 476501}, {t: 1587691025791, v: 476849}, {t: 1587691040800, v: 477020}, {t: 1587691055791, v: 477280}, {t: 1587691070791, v: 477549}, {t: 1587691085791, v: 477758}, {t: 1587691100817, v: 477960}, {t: 1587691115791, v: 478261}, {t: 1587691130791, v: 478559}, {t: 1587691145791, v: 478704},
		{t: 1587691160804, v: 478950}, {t: 1587691175791, v: 479173}, {t: 1587691190791, v: 479368}, {t: 1587691205791, v: 479625}, {t: 1587691220805, v: 479866}, {t: 1587691235791, v: 480008}, {t: 1587691250791, v: 480155}, {t: 1587691265791, v: 480472}, {t: 1587691280811, v: 480598}, {t: 1587691295791, v: 480771}, {t: 1587691310791, v: 480996},
		{t: 1587691325791, v: 481200}, {t: 1587691340803, v: 481381}, {t: 1587691355791, v: 481584}, {t: 1587691370791, v: 481759}, {t: 1587691385791, v: 482003}, {t: 1587691400803, v: 482189}, {t: 1587691415791, v: 482457}, {t: 1587691430791, v: 482623}, {t: 1587691445791, v: 482768}, {t: 1587691460804, v: 483036}, {t: 1587691475791, v: 483322},
		{t: 1587691490791, v: 483566}, {t: 1587691505791, v: 483709}, {t: 1587691520807, v: 483838}, {t: 1587691535791, v: 484091}, {t: 1587691550791, v: 484236}, {t: 1587691565791, v: 484454}, {t: 1587691580816, v: 484710}, {t: 1587691595791, v: 484978}, {t: 1587691610791, v: 485271}, {t: 1587691625791, v: 485476}, {t: 1587691640792, v: 485640},
		{t: 1587691655791, v: 485921}, {t: 1587691670791, v: 486201}, {t: 1587691685791, v: 486555}, {t: 1587691700791, v: 486691}, {t: 1587691715791, v: 486831}, {t: 1587691730791, v: 487033}, {t: 1587691745791, v: 487268}, {t: 1587691760803, v: 487370}, {t: 1587691775791, v: 487571}, {t: 1587691790791, v: 487787}, {t: 1587691805791, v: 488036},
		{t: 1587691820791, v: 488241}, {t: 1587691835791, v: 488411}, {t: 1587691850791, v: 488625}, {t: 1587691865791, v: 488868}, {t: 1587691880791, v: 489005}, {t: 1587691895791, v: 489237}, {t: 1587691910791, v: 489545}, {t: 1587691925791, v: 489750}, {t: 1587691940791, v: 489899}, {t: 1587691955791, v: 490048}, {t: 1587691970791, v: 490364},
		{t: 1587691985791, v: 490485}, {t: 1587692000791, v: 490722}, {t: 1587692015791, v: 490866}, {t: 1587692030791, v: 491025}, {t: 1587692045791, v: 491286}, {t: 1587692060816, v: 491543}, {t: 1587692075791, v: 491787}, {t: 1587692090791, v: 492065}, {t: 1587692105791, v: 492223}, {t: 1587692120816, v: 492501}, {t: 1587692135791, v: 492767},
		{t: 1587692150791, v: 492955}, {t: 1587692165791, v: 493194}, {t: 1587692180792, v: 493402}, {t: 1587692195791, v: 493647}, {t: 1587692210791, v: 493897}, {t: 1587692225791, v: 494117}, {t: 1587692240805, v: 494356}, {t: 1587692255791, v: 494620}, {t: 1587692270791, v: 494762}, {t: 1587692285791, v: 495001}, {t: 1587692300805, v: 495222},
		{t: 1587692315791, v: 495393}, {t: 1587692330791, v: 495662}, {t: 1587692345791, v: 495875}, {t: 1587692360801, v: 496082}, {t: 1587692375791, v: 496196}, {t: 1587692390791, v: 496245}, {t: 1587692405791, v: 496295}, {t: 1587692420791, v: 496365}, {t: 1587692435791, v: 496401}, {t: 1587692450791, v: 496452}, {t: 1587692465791, v: 496491},
		{t: 1587692480791, v: 496544}, {t: 1587692555791, v: 496619}, {t: 1587692570791, v: 496852}, {t: 1587692585791, v: 497052}, {t: 1587692600791, v: 497245}, {t: 1587692615791, v: 497529}, {t: 1587692630791, v: 497697}, {t: 1587692645791, v: 497909}, {t: 1587692660791, v: 498156}, {t: 1587692675803, v: 498466}, {t: 1587692690791, v: 498647},
		{t: 1587692705791, v: 498805}, {t: 1587692720791, v: 499013}, {t: 1587692735805, v: 499169}, {t: 1587692750791, v: 499345}, {t: 1587692765791, v: 499499}, {t: 1587692780791, v: 499731}, {t: 1587692795806, v: 499972}, {t: 1587692810791, v: 500201}, {t: 1587692825791, v: 500354}, {t: 1587692840791, v: 500512}, {t: 1587692855791, v: 500739},
		{t: 1587692870791, v: 500958}, {t: 1587692885791, v: 501190}, {t: 1587692900791, v: 501233}, {t: 1587692915791, v: 501391}, {t: 1587692930791, v: 501649}, {t: 1587692945791, v: 501853}, {t: 1587692960791, v: 502065}, {t: 1587692975791, v: 502239}, {t: 1587692990810, v: 502554}, {t: 1587693005791, v: 502754}, {t: 1587693020791, v: 502938},
		{t: 1587693035791, v: 503141}, {t: 1587693050791, v: 503416}, {t: 1587693065791, v: 503642}, {t: 1587693080791, v: 503873}, {t: 1587693095791, v: 504014}, {t: 1587693110791, v: 504178}, {t: 1587693125821, v: 504374}, {t: 1587693140791, v: 504578}, {t: 1587693155791, v: 504753}, {t: 1587693170791, v: 505043}, {t: 1587693185791, v: 505232},
		{t: 1587693200791, v: 505437}, {t: 1587693215791, v: 505596}, {t: 1587693230791, v: 505923}, {t: 1587693245791, v: 506088}, {t: 1587693260791, v: 506307}, {t: 1587693275791, v: 506518}, {t: 1587693290791, v: 506786}, {t: 1587693305791, v: 507008}, {t: 1587693320803, v: 507260}, {t: 1587693335791, v: 507519}, {t: 1587693350791, v: 507776},
		{t: 1587693365791, v: 508003}, {t: 1587693380791, v: 508322}, {t: 1587693395804, v: 508551}, {t: 1587693410791, v: 508750}, {t: 1587693425791, v: 508994}, {t: 1587693440791, v: 509237}, {t: 1587693455791, v: 509452}, {t: 1587693470791, v: 509702}, {t: 1587693485791, v: 509971}, {t: 1587693500791, v: 510147}, {t: 1587693515791, v: 510471},
		{t: 1587693530816, v: 510666}, {t: 1587693545791, v: 510871}, {t: 1587693560791, v: 511123}, {t: 1587693575791, v: 511303}, {t: 1587693590791, v: 511500},
	}
)

func TestApplyCounterResetsIterator(t *testing.T) {
	for _, tcase := range []struct {
		name string

		chunks [][]sample

		expected []sample
	}{
		{
			name: "series with stale marker",
			chunks: [][]sample{
				{{100, 10}, {200, 20}, {300, 10}, {400, 20}, {400, 5}},
				{{500, 10}, {600, 20}, {700, 30}, {800, 40}, {800, 10}},                // No reset, just downsampling addded sample at the end.
				{{900, 5}, {1000, 10}, {1100, 15}},                                     // Actual reset.
				{{1200, 20}, {1250, math.Float64frombits(value.StaleNaN)}, {1300, 40}}, // No special last sample, no reset.
				{{1400, 30}, {1500, 30}, {1600, 50}},                                   // No special last sample, reset.
			},
			expected: []sample{
				{100, 10}, {200, 20}, {300, 30}, {400, 40}, {500, 45},
				{600, 55}, {700, 65}, {800, 75}, {900, 80}, {1000, 85},
				{1100, 90}, {1200, 95}, {1300, 115}, {1400, 145}, {1500, 145}, {1600, 165},
			},
		},
		{
			name:     "realistic raw data with 2 chunks that have one stale marker",
			chunks:   realisticChkDataWithStaleMarker,
			expected: realisticChkDataWithCounterResetsAfterCounterSeriesIterating,
		},
		{
			// This can easily happen when querying StoreAPI with same data. Counter series should handle this.
			name: "realistic raw data with many overlapping chunks with stale markers",
			chunks: [][]sample{
				realisticChkDataWithStaleMarker[0],
				realisticChkDataWithStaleMarker[0],
				realisticChkDataWithStaleMarker[0],
				realisticChkDataWithStaleMarker[1],
				realisticChkDataWithStaleMarker[1],
				realisticChkDataWithStaleMarker[1],
				realisticChkDataWithStaleMarker[1],
				realisticChkDataWithStaleMarker[1],
				realisticChkDataWithStaleMarker[1],
				realisticChkDataWithStaleMarker[1],
				realisticChkDataWithStaleMarker[1],
				realisticChkDataWithStaleMarker[1],
			},
			expected: realisticChkDataWithCounterResetsAfterCounterSeriesIterating,
		},
		{
			name:   "the same above input (realisticChkDataWithStaleMarker), but after 5m downsampling",
			chunks: [][]sample{realisticChkDataWithCounterResetRes5m[0][AggrCounter]},
			expected: []sample{
				{t: 1587690005791, v: 461968}, {t: 1587690299999, v: 465870}, {t: 1587690599999, v: 469951}, {t: 1587690899999, v: 474726}, {t: 1587691199999, v: 479368},
				{t: 1587691499999, v: 483566}, {t: 1587691799999, v: 487787}, {t: 1587692099999, v: 492065}, {t: 1587692399999, v: 496245}, {t: 1587692699999, v: 498647},
				{t: 1587692999999, v: 502554}, {t: 1587693299999, v: 506786}, {t: 1587693590791, v: 511500},
			},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			var its []chunkenc.Iterator
			for _, c := range tcase.chunks {
				its = append(its, newSampleIterator(c))
			}

			x := NewApplyCounterResetsIterator(its...)

			var res []sample
			for x.Next() {
				t, v := x.At()
				res = append(res, sample{t, v})
			}
			testutil.Ok(t, x.Err())
			testutil.Equals(t, tcase.expected, res)

			for i := range res[1:] {
				testutil.Assert(t, res[i+1].t >= res[i].t, "sample time %v is not monotonically increasing. previous sample %v is older", res[i+1], res[i])
				testutil.Assert(t, res[i+1].v >= res[i].v, "sample value %v is not monotonically increasing. previous sample %v is larger", res[i+1], res[i])
			}
		})
	}

}

func TestCounterSeriesIteratorSeek(t *testing.T) {
	chunks := [][]sample{
		{{100, 10}, {200, 20}, {300, 10}, {400, 20}, {400, 5}},
	}

	exp := []sample{
		{200, 20}, {300, 30}, {400, 40},
	}

	var its []chunkenc.Iterator
	for _, c := range chunks {
		its = append(its, newSampleIterator(c))
	}

	var res []sample
	x := NewApplyCounterResetsIterator(its...)

	ok := x.Seek(150)
	testutil.Assert(t, ok, "Seek should return true")
	testutil.Ok(t, x.Err())
	for {
		ts, v := x.At()
		res = append(res, sample{ts, v})

		ok = x.Next()
		if !ok {
			break
		}
	}
	testutil.Equals(t, exp, res)
}

func TestCounterSeriesIteratorSeekExtendTs(t *testing.T) {
	chunks := [][]sample{
		{{100, 10}, {200, 20}, {300, 10}, {400, 20}, {400, 5}},
	}

	var its []chunkenc.Iterator
	for _, c := range chunks {
		its = append(its, newSampleIterator(c))
	}

	x := NewApplyCounterResetsIterator(its...)

	ok := x.Seek(500)
	testutil.Assert(t, !ok, "Seek should return false")
}

func TestCounterSeriesIteratorSeekAfterNext(t *testing.T) {
	chunks := [][]sample{
		{{100, 10}},
	}
	exp := []sample{
		{100, 10},
	}

	var its []chunkenc.Iterator
	for _, c := range chunks {
		its = append(its, newSampleIterator(c))
	}

	var res []sample
	x := NewApplyCounterResetsIterator(its...)

	x.Next()

	ok := x.Seek(50)
	testutil.Assert(t, ok, "Seek should return true")
	testutil.Ok(t, x.Err())
	for {
		ts, v := x.At()
		res = append(res, sample{ts, v})

		ok = x.Next()
		if !ok {
			break
		}
	}
	testutil.Equals(t, exp, res)
}

func TestSamplesFromTSDBSamples(t *testing.T) {
	for _, tcase := range []struct {
		name string

		input []tsdbutil.Sample

		expected []sample
	}{
		{
			name:     "empty",
			input:    []tsdbutil.Sample{},
			expected: []sample{},
		},
		{
			name:     "one sample",
			input:    []tsdbutil.Sample{testSample{1, 1}},
			expected: []sample{{1, 1}},
		},
		{
			name:     "multiple samples",
			input:    []tsdbutil.Sample{testSample{1, 1}, testSample{2, 2}, testSample{3, 3}, testSample{4, 4}, testSample{5, 5}},
			expected: []sample{{1, 1}, {2, 2}, {3, 3}, {4, 4}, {5, 5}},
		},
	} {
		t.Run(tcase.name, func(t *testing.T) {
			actual := SamplesFromTSDBSamples(tcase.input)
			testutil.Equals(t, tcase.expected, actual)
		})
	}
}

// testSample implements tsdbutil.Sample interface.
type testSample struct {
	t int64
	v float64
}

func (s testSample) T() int64 {
	return s.t
}

func (s testSample) V() float64 {
	return s.v
}

type sampleIterator struct {
	l []sample
	i int
}

func newSampleIterator(l []sample) *sampleIterator {
	return &sampleIterator{l: l, i: -1}
}

func (it *sampleIterator) Err() error {
	return nil
}

func (it *sampleIterator) Next() bool {
	if it.i >= len(it.l)-1 {
		return false
	}
	it.i++
	return true
}

func (it *sampleIterator) Seek(int64) bool {
	panic("unexpected")
}

func (it *sampleIterator) At() (t int64, v float64) {
	return it.l[it.i].t, it.l[it.i].v
}

// memBlock is an in-memory block that implements a subset of the tsdb.BlockReader interface
// to allow tsdb.StreamedBlockWriter to persist the data as a block.
type memBlock struct {
	// Dummies to implement unused methods.
	tsdb.IndexReader

	symbols  map[string]struct{}
	postings []uint64
	series   []*series
	chunks   []chunkenc.Chunk

	numberOfChunks uint64

	minTime, maxTime int64
}

type series struct {
	lset   labels.Labels
	chunks []chunks.Meta
}

func newMemBlock() *memBlock {
	return &memBlock{symbols: map[string]struct{}{}, minTime: -1, maxTime: -1}
}

func (b *memBlock) addSeries(s *series) {
	sid := uint64(len(b.series))
	b.postings = append(b.postings, sid)
	b.series = append(b.series, s)

	for _, l := range s.lset {
		b.symbols[l.Name] = struct{}{}
		b.symbols[l.Value] = struct{}{}
	}

	for i, cm := range s.chunks {
		if b.minTime == -1 || cm.MinTime < b.minTime {
			b.minTime = cm.MinTime
		}
		if b.maxTime == -1 || cm.MaxTime < b.maxTime {
			b.maxTime = cm.MaxTime
		}
		s.chunks[i].Ref = b.numberOfChunks
		b.chunks = append(b.chunks, cm.Chunk)
		b.numberOfChunks++
	}
}

func (b *memBlock) MinTime() int64 {
	if b.minTime == -1 {
		return 0
	}

	return b.minTime
}

func (b *memBlock) MaxTime() int64 {
	if b.maxTime == -1 {
		return 0
	}

	return b.maxTime
}

func (b *memBlock) Meta() tsdb.BlockMeta {
	return tsdb.BlockMeta{}
}

func (b *memBlock) Postings(name string, val ...string) (index.Postings, error) {
	allName, allVal := index.AllPostingsKey()

	if name != allName || val[0] != allVal {
		return nil, errors.New("unexpected call to Postings() that is not AllVall")
	}
	sort.Slice(b.postings, func(i, j int) bool {
		return labels.Compare(b.series[b.postings[i]].lset, b.series[b.postings[j]].lset) < 0
	})
	return index.NewListPostings(b.postings), nil
}

func (b *memBlock) Series(id uint64, lset *labels.Labels, chks *[]chunks.Meta) error {
	if id >= uint64(len(b.series)) {
		return errors.Wrapf(storage.ErrNotFound, "series with ID %d does not exist", id)
	}
	s := b.series[id]

	*lset = append((*lset)[:0], s.lset...)
	*chks = append((*chks)[:0], s.chunks...)

	return nil
}

func (b *memBlock) Chunk(id uint64) (chunkenc.Chunk, error) {
	if id >= b.numberOfChunks {
		return nil, errors.Wrapf(storage.ErrNotFound, "chunk with ID %d does not exist", id)
	}

	return b.chunks[id], nil
}

func (b *memBlock) Symbols() index.StringIter {
	res := make([]string, 0, len(b.symbols))
	for s := range b.symbols {
		res = append(res, s)
	}
	sort.Strings(res)
	return index.NewStringListIter(res)
}

func (b *memBlock) SortedPostings(p index.Postings) index.Postings {
	return p
}

func (b *memBlock) Index() (tsdb.IndexReader, error) {
	return b, nil
}

func (b *memBlock) Chunks() (tsdb.ChunkReader, error) {
	return b, nil
}

func (b *memBlock) Tombstones() (tombstones.Reader, error) {
	return emptyTombstoneReader{}, nil
}

func (b *memBlock) Close() error {
	return nil
}

func (b *memBlock) Size() int64 {
	return 0
}

type emptyTombstoneReader struct{}

func (emptyTombstoneReader) Get(ref uint64) (tombstones.Intervals, error)        { return nil, nil }
func (emptyTombstoneReader) Iter(func(uint64, tombstones.Intervals) error) error { return nil }
func (emptyTombstoneReader) Total() uint64                                       { return 0 }
func (emptyTombstoneReader) Close() error                                        { return nil }
