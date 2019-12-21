package cache

import (
	"reflect"
	"sort"
	"testing"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/discovery/targetgroup"
)

func TestCacheAddresses(t *testing.T) {
	tgs := make(map[string]*targetgroup.Group)
	tgs["g1"] = &targetgroup.Group{
		Targets: []model.LabelSet{
			{model.AddressLabel: "localhost:9090"},
			{model.AddressLabel: "localhost:9091"},
			{model.AddressLabel: "localhost:9092"},
		},
	}
	tgs["g2"] = &targetgroup.Group{
		Targets: []model.LabelSet{
			{model.AddressLabel: "localhost:9091"},
			{model.AddressLabel: "localhost:9092"},
			{model.AddressLabel: "localhost:9093"},
		},
	}

	c := &Cache{tgs: tgs}

	expected := []string{
		"localhost:9090",
		"localhost:9091",
		"localhost:9092",
		"localhost:9093",
	}

	got := c.Addresses()
	sort.Slice(got, func(i, j int) bool {
		return i < j
	})
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, want %v", got, expected)
	}
}
