package snowflakeid

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

var (
	InvalidSystemClock = errors.New("invalid system clock")
)

func NewSnowflakeFactory(bitLenWorkerID, bitLenSequence, gapMs uint, startTime time.Time) *SnowflakeFactory {

	return &SnowflakeFactory{
		bitLenWorkerID:  bitLenWorkerID,
		bitLenSequence:  bitLenSequence,
		bitLenTimestamp: 63 - bitLenSequence - bitLenWorkerID,
		startTime:       startTime,
		unit:            time.Duration(gapMs) * time.Millisecond,
	}
}

type SnowflakeFactory struct {
	bitLenWorkerID, bitLenTimestamp, bitLenSequence uint
	startTime                                       time.Time
	unit                                            time.Duration
}

func (f *SnowflakeFactory) MaskSequence(sequence uint32) uint32 {
	return (sequence + 1) & f.MaxSequence()
}

func (f *SnowflakeFactory) FlakeTimestamp(t time.Time) uint64 {
	return uint64(t.UnixNano() / int64(f.unit))
}

func (f *SnowflakeFactory) SleepTime(overtime time.Duration) time.Duration {
	return overtime*f.unit - time.Duration(time.Now().UnixNano())%f.unit*time.Nanosecond
}

func (f *SnowflakeFactory) BuildID(workerID uint32, elapsedTime uint64, sequence uint32) uint64 {
	return elapsedTime<<(f.bitLenSequence+f.bitLenWorkerID) | uint64(sequence)<<f.bitLenWorkerID | uint64(workerID)
}

func (f *SnowflakeFactory) MaxSequence() uint32 {
	return 1<<f.bitLenSequence - 1
}

func (f *SnowflakeFactory) MaxWorkerID() uint32 {
	return 1<<f.bitLenWorkerID - 1
}

func (f *SnowflakeFactory) MaxTime() time.Time {
	maxTime := uint64(1<<f.bitLenTimestamp - 1)
	return time.Unix(int64(time.Duration(f.FlakeTimestamp(f.startTime)+maxTime)*f.unit/time.Second), 0)
}

func (f *SnowflakeFactory) NewSnowflake(workerID uint32) (*Snowflake, error) {
	maxWorkerID := f.MaxWorkerID()
	if workerID > maxWorkerID {
		return nil, fmt.Errorf("worker id can't be large than %d", maxWorkerID)
	}
	return &Snowflake{f: f, workerID: workerID, syncMutex: &sync.Mutex{}}, nil
}

func NewSnowflake(workerID uint32) (*Snowflake, error) {
	startTime, _ := time.Parse(time.RFC3339, "2010-11-04T01:42:54.657Z")
	return NewSnowflakeFactory(10, 12, 1, startTime).NewSnowflake(workerID)
}

type Snowflake struct {
	f           *SnowflakeFactory
	workerID    uint32
	elapsedTime uint64
	sequence    uint32
	syncMutex   *sync.Mutex
}

func (sf *Snowflake) WorkerID() uint32 {
	return sf.workerID
}

func (sf *Snowflake) ID() (uint64, error) {
	sf.syncMutex.Lock()
	defer sf.syncMutex.Unlock()

	last := sf.elapsedTime
	t := sf.f.FlakeTimestamp(sf.f.startTime)

	current := sf.f.FlakeTimestamp(time.Now()) - t

	if current < last {
		current = sf.f.FlakeTimestamp(time.Now()) - t
		if current < last {
			return 0, InvalidSystemClock
		}
	}

	sequence := uint32(0)

	if last == current {
		sequence = sf.f.MaskSequence(sf.sequence)
		if sequence == 0 {
			current++
			overtime := current - last
			time.Sleep(sf.f.SleepTime(time.Duration(overtime)))
			sequence = generateRandomSequence(9)
		}
	} else {
		sequence = generateRandomSequence(9)
	}

	sf.elapsedTime = current
	sf.sequence = sequence

	return sf.f.BuildID(sf.workerID, sf.elapsedTime, sf.sequence), nil
}

func generateRandomSequence(n int32) uint32 {
	return uint32(rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(n))
}
