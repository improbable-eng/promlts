package storepb

import (
	"github.com/prometheus/prometheus/pkg/labels"
)

// LabelsCompare is a function that compares two label sets.
// The result will be 0 if a==b, <0 if a < b, and >0 if a > b.
type LabelsCompare func(a, b labels.Labels) int

// NewReplicaAwareLabelsCompareFunc returns LabelsCompare function that compares the two label sets while ignoring
// replica labels. If they are the same it compares with replica labels. The result will be 0 if a==b, <0 if a < b, and >0 if a > b.
func NewReplicaAwareLabelsCompareFunc(replicaLabels map[string]struct{}) LabelsCompare {
	return func(a, b labels.Labels) int {
		withReplicaDecision := 0
		ai, bi := 0, 0
		for ai < len(a) && bi < len(b) {
			cmp := cmpLabel(ai, bi, a, b)
			_, arep := replicaLabels[a[ai].Name]
			if arep {
				if withReplicaDecision == 0 {
					withReplicaDecision = cmp
				}
				ai++
			}

			_, brep := replicaLabels[b[bi].Name]
			if brep {
				if withReplicaDecision == 0 {
					withReplicaDecision = cmp
				}
				bi++
			}

			if arep || brep {
				continue
			}
			if cmp != 0 {
				return cmp
			}

			ai++
			bi++
		}

		for ; ai < len(a); ai++ {
			// b finished earlier, so it's "smaller". Check if it does not have replica labels too.
			_, arep := replicaLabels[a[ai].Name]
			if !arep {
				// b is still smaller.
				return 1
			}
		}

		for ; bi < len(b); bi++ {
			// a finished earlier, so it's "smaller". Check if it does not have replica labels too.
			_, brep := replicaLabels[b[bi].Name]
			if !brep {
				// a is still smaller.
				return -1
			}
		}

		if withReplicaDecision != 0 {
			return withReplicaDecision
		}

		return len(a) - len(b)
	}
}

func cmpLabel(ai, bi int, a, b labels.Labels) int {
	if a[ai].Name != b[bi].Name {
		if a[ai].Name < b[bi].Name {
			return -1
		}
		return 1
	}
	if a[ai].Value != b[bi].Value {
		if a[ai].Value < b[bi].Value {
			return -1
		}
		return 1
	}
	return 0
}
