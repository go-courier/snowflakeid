package snowflakeid

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	bitsTotal     = 64
	bitsTimestamp = 41
	bitsWorkerID  = 10
	bitsSequence  = 12

	twepoch      = uint64(1288834974657)     // Thu, 04 Nov 2010 01:42:54 657ms GMT
	maskSequence = -1 ^ (-1 << bitsSequence) // MAX 4095

	workerIDLeftShift   = bitsSequence
	timestampRightShift = bitsTotal - bitsTimestamp - bitsWorkerID - bitsSequence
	timestampLeftShift  = bitsSequence + bitsWorkerID + timestampRightShift
	maxWorkerID         = -1 ^ (-1 << bitsWorkerID)
)

var (
	WorkerIDToLarge    = errors.New("worker id can't be large than " + strconv.FormatInt(maxWorkerID, 10))
	InvalidSystemClock = errors.New("invalid system clock")
)

func NewSnowflake(workerID uint32) (*Snowflake, error) {
	if workerID > maxWorkerID {
		return nil, WorkerIDToLarge
	}
	return &Snowflake{workerID: workerID, syncMutex: &sync.Mutex{}}, nil
}

type Snowflake struct {
	workerID      uint32
	nextWorkerID  uint32
	lastTimestamp uint64
	sequence      uint32
	syncMutex     *sync.Mutex
}

func (g *Snowflake) WorkerID() uint32 {
	return g.workerID
}

func (g *Snowflake) SetWorkerID(workerID uint32) error {
	if workerID > maxWorkerID {
		return WorkerIDToLarge
	}
	atomic.StoreUint32(&g.nextWorkerID, workerID)
	return nil
}

func (g *Snowflake) next() (*flake, error) {
	g.syncMutex.Lock()
	defer g.syncMutex.Unlock()

	lastTimeStamp := g.lastTimestamp

	timestamp := unixMillisecond()

	if timestamp < lastTimeStamp {
		timestamp = unixMillisecond()
		if timestamp < lastTimeStamp {
			return nil, InvalidSystemClock
		}
	}

	var sequence uint32
	workerID := g.workerID

	if timestamp == lastTimeStamp {
		sequence = (g.sequence + 1) & maskSequence
		if sequence == 0 {
			timestamp = sleepTillNextMillisecond(timestamp)
			sequence = generateRandomSequence(9)
		}
	} else {
		sequence = generateRandomSequence(9)

		// may switch worker id after timestamp changed
		if g.nextWorkerID != 0 {
			workerID = g.nextWorkerID
			g.nextWorkerID = 0
		}
	}

	g.lastTimestamp = timestamp
	g.workerID = workerID
	g.sequence = sequence

	return newFlake(timestamp, workerID, sequence), nil
}

func unixMillisecond() uint64 {
	return uint64(time.Now().UnixNano() / int64(time.Millisecond))
}

func sleepTillNextMillisecond(timestamp uint64) uint64 {
	t := unixMillisecond()
	for t <= timestamp {
		time.Sleep(100 * time.Microsecond)
		t = unixMillisecond()
	}
	return t
}

func generateRandomSequence(n int32) uint32 {
	return uint32(rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(n))
}

func (g *Snowflake) ID() (uint64, error) {
	param, err := g.next()
	if err != nil {
		return 0, err
	}
	return param.id(), nil
}

func newFlake(timestamp uint64, workerID uint32, sequence uint32) *flake {
	return &flake{timestamp, workerID, sequence}
}

type flake struct {
	timestamp uint64
	workerID  uint32
	sequence  uint32
}

func (p *flake) id() uint64 {
	// 41 bit timestamp part
	timestampPart := (p.timestamp - twepoch) << timestampLeftShift >> timestampRightShift
	// 10 bit worker id
	workerIDPart := uint64(p.workerID) << workerIDLeftShift
	// 12 bit serial number
	noPart := uint64(p.sequence)

	return timestampPart | workerIDPart | noPart
}
