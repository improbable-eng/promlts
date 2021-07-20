package storepb

import (
	"testing"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/thanos-io/thanos/pkg/testutil"
)

func TestReplicaAwareLabelsCompare(t *testing.T) {
	replicaLabels := map[string]struct{}{
		"replica":  {},
		"replica2": {},
	}

	a := labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "replica", Value: "333"}, {Name: "xxx", Value: "444"}}

	for _, tcase := range []struct {
		a, b     labels.Labels
		expected int
	}{
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "110"}, {Name: "bbb", Value: "222"}, {Name: "replica", Value: "333"}, {Name: "xxx", Value: "444"}},
			expected: 1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "233"}, {Name: "replica", Value: "333"}, {Name: "xxx", Value: "444"}},
			expected: -1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bar", Value: "222"}, {Name: "replica", Value: "333"}, {Name: "xxx", Value: "444"}},
			expected: 1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbc", Value: "222"}, {Name: "replica", Value: "333"}, {Name: "xxx", Value: "444"}},
			expected: -1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}},
			expected: 1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "ccc", Value: "333"}, {Name: "replica", Value: "333"}, {Name: "xxx", Value: "444"}},
			expected: 1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "ccc", Value: "333"}, {Name: "xxx", Value: "444"}},
			expected: 1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "t", Value: "333"}, {Name: "xxx", Value: "444"}},
			expected: 1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "replica", Value: "333"}, {Name: "xxx", Value: "444"}},
			expected: 0,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "replica", Value: "332"}, {Name: "xxx", Value: "444"}},
			expected: 1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "replica", Value: "333"}, {Name: "replica2", Value: "333"}, {Name: "xxx", Value: "444"}},
			expected: 1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "xxx", Value: "333"}},
			expected: 1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "xxx", Value: "555"}},
			expected: -1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "xxx", Value: "444"}},
			expected: -1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "replica", Value: "333"}, {Name: "xxx", Value: "444"}, {Name: "replica2", Value: "333"}},
			expected: -1,
		},
		{
			a: a, b: labels.Labels{{Name: "aaa", Value: "111"}, {Name: "bbb", Value: "222"}, {Name: "xxx", Value: "444"}, {Name: "replica2", Value: "333"}},
			expected: -1,
		},
		{
			a:        labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "d", Value: "4"}},
			b:        labels.Labels{{Name: "a", Value: "1"}, {Name: "c", Value: "3"}, {Name: "replica", Value: "replica-1"}},
			expected: 1,
		},
		{
			a:        labels.Labels{{Name: "replica", Value: "replica-1"}},
			b:        labels.Labels{{Name: "replica", Value: "replica-2"}},
			expected: -1,
		},
		{
			a:        labels.Labels{{Name: "replica", Value: "replica-1"}},
			b:        labels.Labels{{Name: "replica", Value: "replica-1"}},
			expected: 0,
		},
	} {
		t.Run("", func(t *testing.T) {
			testutil.Equals(t, tcase.expected, NewReplicaAwareLabelsCompareFunc(replicaLabels)(tcase.a, tcase.b))
			// Opposite should be true too.
			testutil.Equals(t, -1*tcase.expected, NewReplicaAwareLabelsCompareFunc(replicaLabels)(tcase.b, tcase.a))
		})
	}
}
