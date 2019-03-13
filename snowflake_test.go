package snowflakeid

import (
	"sync"
	"testing"
)

func BenchmarkGenUniqueID(b *testing.B) {
	g, _ := NewSnowflake(1)

	for i := 0; i < b.N; i++ {
		_, err := g.ID()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSnowflake(t *testing.T) {
	suite := NewTestSuite(t, 1000000)
	generator, err := NewSnowflake(1)
	if err != nil {
		t.Fatal(err)
	}

	suite.RunGenerator(generator)
	suite.ExpectN(suite.N)
}

func TestSnowflake_InMultiGoroutine(t *testing.T) {
	suite := NewTestSuite(t, 100)
	generator, err := NewSnowflake(1)
	if err != nil {
		t.Fatal(err)
	}

	n := 1000

	wg := &sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			suite.RunGenerator(generator)
		}()
	}

	wg.Wait()

	suite.ExpectN(suite.N * n)
}

func TestSnowflake_WithDifferentWorkerID(t *testing.T) {
	suite := NewTestSuite(t, 100000)

	wg := &sync.WaitGroup{}

	n := 10

	for i := 0; i < n; i++ {
		generator, err := NewSnowflake(uint32(i + 1))
		if err != nil {
			t.Fatal(err)
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			suite.RunGenerator(generator)
		}()
	}

	wg.Wait()

	suite.ExpectN(suite.N * n)
}

func TestSnowflake_DynamicChangeWorkerID(t *testing.T) {
	g, err := NewSnowflake(1)
	if err != nil {
		t.Fatal(err)
	}

	ts := uint64(0)
	s := uint32(0)
	changed := false
	breakpoint := false

	for i := 1; i <= 1000000; i++ {
		flake, _ := g.next()
		if ts == 0 || ts == flake.timestamp {
			if !changed {
				ts = flake.timestamp
				err := g.SetWorkerID(2)
				if err != nil {
					t.Fatal(err)
				}
				changed = true
			}
			if flake.workerID != 1 {
				t.Fatalf("worker id should not be changed in same timestamp loop")
			}
			if flake.sequence < s {
				t.Fatalf("sequence should auto increase")
			}
		} else {
			if !breakpoint {
				if flake.workerID != 2 {
					t.Fatalf("worker id should be changed, but got %d", flake.workerID)
				}
				if flake.sequence > s {
					t.Fatalf("sequence %d should reset, last %d", flake.sequence, s)
				}
				breakpoint = true
			}
		}

		s = flake.sequence
	}
}

func TestToID(t *testing.T) {
	timestampBits := uint(41)
	workerIDBits := uint(10)
	sequenceBits := uint(12)

	t.Run("Max", func(t *testing.T) {
		timestamp := uint64(1<<timestampBits - 1)
		workerID := uint32(1<<workerIDBits - 1)
		sequence := uint32(1<<sequenceBits - 1)

		id := newFlake(timestamp, workerID, sequence).id()

		if id<<(64-sequenceBits)>>(64-sequenceBits) != uint64(sequence) {
			t.Fatal("sequence bits incorrect")
		}

		if id<<(64-sequenceBits-workerIDBits)>>(64-workerIDBits) != uint64(workerID) {
			t.Fatal("worker id bits incorrect")
		}

		if id<<(64-sequenceBits-workerIDBits-timestampBits)>>(64-timestampBits) != timestamp-twepoch {
			t.Fatal("timestamp bits incorrect")
		}
	})

	t.Run("Min", func(t *testing.T) {
		timestamp := uint64(twepoch)
		workerID := uint32(0)
		sequence := uint32(0)

		id := newFlake(timestamp, workerID, sequence).id()

		if id<<(64-sequenceBits)>>(64-sequenceBits) != uint64(sequence) {
			t.Fatal("sequence bits incorrect")
		}
		if id<<(64-sequenceBits-workerIDBits)>>(64-workerIDBits) != uint64(workerID) {
			t.Fatal("worker id bits incorrect")
		}
		if id<<(64-sequenceBits-workerIDBits-timestampBits)>>(64-timestampBits) != timestamp-twepoch {
			t.Fatal("timestamp bits incorrect")
		}
	})
}

func NewTestSuite(t *testing.T, n int) *Suite {
	return &Suite{
		T: t,
		N: n,
	}
}

type Suite struct {
	*testing.T
	N int
	sync.Map
}

func (s *Suite) ExpectN(n int) {
	c := 0
	s.Range(func(key, value interface{}) bool {
		c++
		return true
	})
	if c != n {
		s.Fatalf("expect generated %d, but go %d", n, c)
	}
}

func (s *Suite) RunGenerator(generator *Snowflake) {
	for i := 1; i <= s.N; i++ {
		id, err := generator.ID()
		if err != nil {
			s.Fatal(err)
		}
		s.Store(id, true)
	}
}
