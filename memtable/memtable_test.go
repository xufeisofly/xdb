package memtable_test

import (
	"testing"

	"github.com/xufeisofly/xdb/memtable"
)

type kv struct {
	Key       string
	Timestamp int64
	Value     string
}

func TestInsertAndFind(t *testing.T) {
	tests := []struct {
		name          string
		insert        []kv
		findKey       string
		expectedValue string
	}{
		{
			"test find key, found",
			[]kv{{"a", 1, "value1"}, {"b", 1, "value2"}},
			"a",
			"value1",
		}, {
			"test find key, not found",
			[]kv{{"a", 1, "value1"}},
			"b",
			"",
		},
	}

	for _, tt := range tests {
		m := memtable.New()
		for _, kv := range tt.insert {
			m.Insert(kv.Key, kv.Value, kv.Timestamp)
		}
		actualValue, _ := m.Find(tt.findKey)
		if actualValue != tt.expectedValue {
			t.Errorf("actual: %s, expect: %s", actualValue, tt.expectedValue)
		}
	}
}
