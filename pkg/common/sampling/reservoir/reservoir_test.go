package reservoir

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDeterministicUnweightedReservoir(t *testing.T) {

	// For seed 0, this is the set of distributions our algorithm happens to
	// produce.  These results will not be stable across algorithmic changes;
	// if you need to change the expected distribution, try a few seeds and
	// verify that the results are close to the 4:3:2:1 ratio of the input.
	rand.Seed(0)
	expectedDist := map[string]int{"a": 3946, "b": 3028, "c": 2001, "d": 1025}

	res := NewReservoir(10000, nil, nil)

	addN := func(value string, occurrences int) {
		for i := 0; i < occurrences; i++ {
			res.Add(value, 1)
		}
	}
	addN("a", 400000)
	addN("b", 300000)
	addN("c", 200000)
	addN("d", 100000)

	winners, weight, discards, discardsW := res.Close()

	if weight != 10000 {
		t.Fatalf("didn't get expected weight: expected 10000, got %d", weight)
	}

	if discards != 990000 || discardsW != 990000 {
		t.Fatalf("didn't get expected discard rates: expected 990000 on each, but got %d discarded items and %d discarded weight", discards, discardsW)
	}

	if len(winners) != 10000 {
		t.Fatalf("didn't get the expected number of winners: expected 10000, got %d", len(winners))
	}

	freqDist := make(map[string]int)
	for _, winner := range winners {
		ws := winner.(string)
		freqDist[ws]++
	}
	assert.Equal(t, expectedDist, freqDist, "unexpected frequency distribution")
}

func TestProbabilisticUnweightedReservoir(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	expectedDist := map[string]int{"a": 4000, "b": 3000, "c": 2000, "d": 1000}

	for i := 0; i < 1; i++ {
		res := NewReservoir(10000, nil, nil)

		addN := func(value string, occurrences int) {
			for i := 0; i < occurrences; i++ {
				res.Add(value, 1)
			}
		}
		addN("a", 400000)
		addN("b", 300000)
		addN("c", 200000)
		addN("d", 100000)

		winners, weight, discards, discardsW := res.Close()

		if weight != 10000 {
			t.Fatalf("didn't get expected weight: expected 10000, got %d", weight)
		}

		if discards != 990000 || discardsW != 990000 {
			t.Fatalf("didn't get expected discard rates: expected 990000 on each, but got %d discarded items and %d discarded weight", discards, discardsW)
		}

		if len(winners) != 10000 {
			t.Fatalf("didn't get the expected number of winners: expected 10000, got %d", len(winners))
		}

		freqDist := make(map[string]int)
		for _, winner := range winners {
			ws := winner.(string)
			freqDist[ws]++
		}

		for k, v := range freqDist {
			diff := math.Abs(float64(v - expectedDist[k]))
			if diff/float64(expectedDist[k]) > .1 {
				t.Fatalf("Unexpected value for key %s: expected %d, got %d (%f)\n", k, expectedDist[k], v, float64(diff)/float64(expectedDist[k]))
			}
		}
	}
}

func TestProbabilisticWeightedReservoir(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	expectedDist := map[string]int{"a": 750, "b": 1000, "c": 1500, "d": 3000}

	for i := 0; i < 1; i++ {
		res := NewReservoir(12000, nil, nil)

		addN := func(value string, weight uint64, occurrences int) {
			for i := 0; i < occurrences; i++ {
				res.Add(value, weight)
			}
		}
		addN("a", 4, 100000)
		addN("b", 3, 100000)
		addN("c", 2, 100000)
		addN("d", 1, 100000)

		winners, weight, discards, discardsW := res.Close()

		if math.Abs(float64(12000-int(weight))) > 4 {
			t.Fatalf("didn't get expected weight: expected 12000, got %d %d %f %f", weight, weight-12000, float64(weight-12000), math.Abs(float64(weight-1200)))
		}

		if discards+uint64(len(winners)) != 400000 {
			t.Fatalf("didn't get expected number of entries: discards + winners should be 400000, but discards is %d and winners is %d", discards, len(winners))
		}

		if weight+discardsW != 1000000 {
			t.Fatalf("didn't get expected number of entries: discardsW + weight should be 1000000, but discardsW is %d and weight is %d", discardsW, weight)
		}

		freqDist := make(map[string]int)
		for _, winner := range winners {
			ws := winner.(string)
			freqDist[ws]++
		}

		for k, v := range freqDist {
			diff := math.Abs(float64(v - expectedDist[k]))
			if diff/float64(expectedDist[k]) > .15 {
				t.Fatalf("Unexpected value for key %s: expected %d, got %d (%f)\n", k, expectedDist[k], v, float64(diff)/float64(expectedDist[k]))
			}
		}
	}
}

func TestProbabilisticTemplateReservoir(t *testing.T) {

	rand.Seed(time.Now().UnixNano())
	expectedDist := map[string]int{"a": 750, "b": 1000, "c": 1500, "d": 3000}

	for i := 0; i < 1; i++ {
		res := NewReservoir(12000, nil, nil)

		res.AddMust("e", 1)
		addN := func(value string, weight uint64, occurrences int) {
			for i := 0; i < occurrences; i++ {
				res.Add(value, weight)
			}
		}
		addN("a", 4, 100000)
		addN("b", 3, 100000)
		addN("c", 2, 100000)
		addN("d", 1, 100000)
		res.AddMust("e", 1)

		winners, weight, discards, discardsW := res.Close()

		if math.Abs(float64(12000-int(weight))) > 4 {
			t.Fatalf("didn't get expected weight: expected 12000, got %d", weight)
		}

		if discards+uint64(len(winners)) != 400002 {
			t.Fatalf("didn't get expected number of entries: discards + winners should be 400002, but discards is %d and winners is %d", discards, len(winners))
		}

		if weight+discardsW != 1000002 {
			t.Fatalf("didn't get expected number of entries: discardsW + weight should be 1000002, but discardsW is %d and weight is %d", discardsW, weight)
		}

		freqDist := make(map[string]int)
		for _, winner := range winners {
			ws := winner.(string)
			freqDist[ws]++
		}

		if freqDist["e"] != 2 {
			t.Fatalf("missing values added with AddMust: %d", freqDist["e"])
		}

		for k, v := range expectedDist {
			diff := math.Abs(float64(freqDist[k] - v))
			if diff/float64(v) > .15 {
				t.Fatalf("Unexpected value for key %s: expected %d, got %d (%f)\n", k, expectedDist[k], v, float64(diff)/float64(expectedDist[k]))
			}
		}
	}
}
