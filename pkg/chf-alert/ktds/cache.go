package ktds

import (
	"sync"
	"time"
)

type cachedValue struct {
	sync.RWMutex
	expiration time.Time
	value      interface{}
}

func newCachedValue() *cachedValue {
	return &cachedValue{}
}

func (p *cachedValue) Get() (interface{}, bool) {
	p.RLock()
	v, exp := p.value, p.expiration
	p.RUnlock()
	if v == nil || exp.Before(time.Now()) {
		return nil, false
	}
	return v, true
}

func (p *cachedValue) Set(v interface{}, ttl time.Duration) {
	p.Lock()
	p.value = v
	p.expiration = time.Now().Add(ttl)
	p.Unlock()
}
