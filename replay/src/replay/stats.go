package replay

import (
	"time"
)

type Latency struct {
	OpType  OpType
	Latency time.Duration
}

// Collect the stats during op execution.
type IStatsCollector interface {
	StartOp(opType OpType)
	EndOp()

	// How many ops have been captured.
	Count(opType OpType) int64
	// ops/sec for a given op type.
	OpsSec(opType OpType) float64
	// The average latency, which can give you a rough idea of the performance.
	// For fine-grain performance analysis, please enable latency sampling
	// and do the latency analysis by other means.
	LatencyInMs(opType OpType) float64

	// Enable the sampling for latency analysis. Sampled latency will be sent
	// out via a channel.
	EnableLatencySampling(sampleRate float64, latencyChannel chan Latency)
}

type StatsCollector struct {
	counts    map[OpType]int64
	durations map[OpType]time.Duration

	total int
	// sample rate will be among [0-100]
	sampleRate  int
	epoch       *time.Time
	lastOp      *OpType
	latencyChan chan Latency
}

func NewStatsCollector() *StatsCollector {
	counts := map[OpType]int64{}
	durations := map[OpType]time.Duration{}
	for _, opType := range AllOpTypes {
		counts[opType] = 0
		durations[opType] = 0
	}
	collector := &StatsCollector{
		counts:    counts,
		durations: durations,
	}
	return collector
}

func (s *StatsCollector) StartOp(opType OpType) {
	if s.total%10000 < s.sampleRate {
		now := time.Now()
		s.epoch = &now
		s.lastOp = &opType
	}
	s.total++
}

func (s *StatsCollector) EndOp() {
	if s.epoch == nil {
		return
	}

	duration := time.Now().Sub(*s.epoch)
	s.durations[*s.lastOp] += duration
	s.counts[*s.lastOp]++
	if s.latencyChan != nil {
		s.latencyChan <- Latency{*s.lastOp, duration}
	}
	s.epoch = nil
	s.lastOp = nil
}

func (s *StatsCollector) Count(opType OpType) int64 {
	return s.counts[opType]
}

func (s *StatsCollector) TotalTime(opType OpType) time.Duration {
	return s.durations[opType]
}
func (s *StatsCollector) OpsSec(opType OpType) float64 {
	nano := s.TotalTime(opType).Nanoseconds()
	if nano == 0 {
		return 0
	}
	return float64(s.counts[opType]) * float64(time.Second) / float64(nano)
}

func (s *StatsCollector) LatencyInMs(opType OpType) float64 {
	count := float64(s.counts[opType])
	if count == 0 {
		return 0
	}
	sec := s.TotalTime(opType).Seconds()
	return sec / count * 1000
}
func (s *StatsCollector) EnableLatencySampling(
	sampleRate float64, latencyChannel chan Latency) {
	s.sampleRate = int(10000 * sampleRate)
	s.latencyChan = latencyChannel
}

// Combine the stats collected by multiple stats to one.
func CombineStats(statsList ...*StatsCollector) *StatsCollector {
	newStats := NewStatsCollector()

	for _, opType := range AllOpTypes {
		for _, stats := range statsList {
			newStats.counts[opType] += stats.counts[opType]
			newStats.durations[opType] += stats.durations[opType]
		}
	}
	return newStats
}

// A Stats collector that does nothing.
type NullStatsCollector struct {
}

func (e *NullStatsCollector) StartOp(opType OpType) {
}

func (e *NullStatsCollector) EndOp() {
}

func (e *NullStatsCollector) Count(opType OpType) int64 {
	return 0
}

func (e *NullStatsCollector) TotalTime(opType OpType) time.Duration {
	return 0
}
func (e *NullStatsCollector) OpsSec(opType OpType) float64 {
	return 0
}
func (e *NullStatsCollector) LatencyInMs(opType OpType) float64 {
	return 0
}
func (e *NullStatsCollector) EnableLatencySampling(
	sampleRate float64, latencyChannel chan Latency) {
}
