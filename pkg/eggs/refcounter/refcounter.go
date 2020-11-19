package refcounter

type RefCounter interface {
	Inc(key interface{})
	Dec(key interface{})
	Above(count int) []interface{}
	Below(count int) []interface{}
}

type refCounter struct {
	counts map[interface{}]int
}

func NewRefCounter() *refCounter {
	return &refCounter{
		counts: make(map[interface{}]int, 0),
	}
}

func (rc *refCounter) Inc(key interface{}) {
	value, ok := rc.counts[key]
	if !ok {
		rc.counts[key] = 1
	} else {
		rc.counts[key] = value + 1
	}
}

func (rc *refCounter) Dec(key interface{}) {
	value, ok := rc.counts[key]
	if !ok {
		rc.counts[key] = -1
	} else {
		rc.counts[key] = value - 1
	}
}

func (rc *refCounter) Iter(f func(key interface{}, count int) error) error {
	for key, value := range rc.counts {
		if err := f(key, value); err != nil {
			return err
		}
	}
	return nil
}
