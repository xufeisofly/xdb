package memtable

import (
	"math"
	"math/rand"
	"sync"
)

type Memtable struct {
	*SkipList
}

func New() *Memtable {
	sl := &SkipList{
		head: &node{
			next: make([]*node, MAX_LEVEL),
		},
	}
	return &Memtable{sl}
}

type ValueType int64

const (
	ValueTypeValue ValueType = iota
	ValueTypeDelete
)

const MAX_LEVEL = 16

type SkipList struct {
	head *node
	mu   sync.Mutex
}

type node struct {
	key       string
	value     string
	timestamp int64
	next      []*node
}

func (sl *SkipList) Insert(key, value string, timestamp int64) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	prev := make([]*node, MAX_LEVEL+1)
	n := sl.findGreaterOrEqual(key, timestamp, prev)
	if n != nil && n.key == key && n.timestamp == timestamp {
		panic("key already existed")
	}

	level := sl.randomLevel(0.5)
	newNode := &node{
		key:       key,
		value:     value,
		timestamp: timestamp,
		next:      make([]*node, level+1),
	}

	for i := level; i >= 0; i-- {
		newNode.next[i] = prev[i].next[i]
		prev[i].next[i] = newNode
	}
}

// Find 查询 value By key
func (sl *SkipList) Find(key string) (string, bool) {
	node := sl.findGreaterOrEqual(key, math.MaxInt64, nil)
	if node != nil && node.key == key {
		return node.value, true
	}
	return "", false
}

// func (sl *SkipList) FindRange(startKey, endKey string) []*node {}

// randomLevel 随机生成 level
func (sl *SkipList) randomLevel(p float64) int64 {
	level := 1
	for (rand.Float64() < p) && level < MAX_LEVEL {
		level += 1
	}
	return int64(level)
}

func (sl *SkipList) findGreaterOrEqual(key string, timestamp int64, prev []*node) *node {
	c := sl.head

	var nextAtLevel *node

	for cl := MAX_LEVEL - 1; cl >= 0; cl-- {
		nextAtLevel = c.next[cl] // 取 next 本来是需要保证 atomic 的，但其实在 Insert 中加锁就行了
		for nextAtLevel != nil && (nextAtLevel.key < key || (nextAtLevel.key == key && nextAtLevel.timestamp > timestamp)) {
			c = nextAtLevel
			nextAtLevel = c.next[cl]
		}

		if prev != nil {
			prev[cl] = c
		}
	}

	return nextAtLevel
}
