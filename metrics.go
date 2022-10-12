package main

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/streamingfast/dmetrics"
	"go.uber.org/zap"
)

var metrics = dmetrics.NewSet()

var HeadBlockNumber = metrics.NewHeadBlockNumber("substreams-consumer")
var HeadBlockTime = metrics.NewHeadTimeDrift("substreams-consumer")
var ChainHeadBlockNumber = metrics.NewGauge("chain_head_block_number", "The latest block of the chain as reported by Firehose endpoint")

var FirehoseErrorCount = metrics.NewCounter("firehose_error", "The error count we encountered when interacting with Firehose for which we had to restart the connection loop")
var SubstreamsErrorCount = metrics.NewCounter("substreams_error", "The error count we encountered when interacting with Substreams for which we had to restart the connection loop")

var MessageSizeBytes = metrics.NewCounter("message_size_bytes", "The number of total bytes of message received from the Substreams backend")
var UnknownMessageCount = metrics.NewCounter("unknown_message", "The number of data message received")
var DataMessageCount = metrics.NewCounterVec("data_message", []string{"module"}, "The number of data message received")
var ProgressMessageCount = metrics.NewCounterVec("progress_message", []string{"module"}, "The number of progress message received")

var BackprocessingCompletion = metrics.NewGauge("backprocessing_completion", "Determines if backprocessing is completed, which is if we receive a first data message")
var HeadBlockReached = metrics.NewGauge("head_block_reached", "Determines if head block was reached at some point, once set it will not change anymore however on restart, there might be a delay before it's set back to 1")

var ModuleProgressBlock = metrics.NewGaugeVec("module_progress_last_block", []string{"module"}, "Latest processed range end block for each module")

var StepNewCount = metrics.NewCounter("step_new_count", "How many NEW step message we received")
var StepUndoCount = metrics.NewCounter("step_undo_count", "How many UNDO step message we received")

var OutputMapperCount = metrics.NewCounterVec("output_mapper_count", []string{"module"}, "The number Mapper output type (a.k.a Data) per module received so far")
var OutputStoreDeltasCount = metrics.NewCounterVec("output_store_deltas_count", []string{"module"}, "The number Store Deltas output type per module received so far")

var OutputMapperSizeBytes = metrics.NewCounterVec("output_mapper_size_bytes", []string{"module"}, "The number Mapper output type (a.k.a Data) per module received so far")
var OutputStoreDeltaSizeBytes = metrics.NewCounterVec("output_store_delta_size_bytes", []string{"module"}, "The number Store Delta output type per module received so far")

// var EntityChangesCount = metrics.NewCounter("entity_change", "The number of entity changes received, only works if module output type is ")

func setup(logger *zap.Logger, metricsListenAddr string, pprofListenAddr string) {
	if metricsListenAddr != "" {
		metrics.Register()

		go dmetrics.Serve(metricsListenAddr)
	}

	if pprofListenAddr != "" {
		go func() {
			err := http.ListenAndServe(pprofListenAddr, nil)
			if err != nil {
				logger.Debug("unable to start profiling server", zap.Error(err), zap.String("listen_addr", pprofListenAddr))
			}
		}()
	}
}

// ValueFromMetric can be used to extract the value of a Prometheus Metric object.
//
// *Important* This for now does not correctly handles `Vec` like metrics since the
// actual logic is to return the first ever value encountered while in a `Vec` metric,
// there is usually N values, one per label.
type ValueFromMetric struct {
	metric prometheus.Collector
	unit   string
}

func NewValueFromMetric(metric prometheus.Collector, unit string) *ValueFromMetric {
	return &ValueFromMetric{metric, unit}
}

func (c *ValueFromMetric) ValueUint() uint64 {
	return uint64(c.ValueFloat())
}

func (c *ValueFromMetric) ValueFloat() float64 {
	metricChan := make(chan prometheus.Metric, 16)
	go func() {
		c.metric.Collect(metricChan)
		close(metricChan)
	}()

	var firstValue *float64
	for value := range metricChan {
		if firstValue != nil {
			// We must fully consume the metric chan, so if the first value has been found, discard any more reading
			continue
		}

		model := new(dto.Metric)
		err := value.Write(model)
		if err != nil {
			panic(err)
		}

		if model.Gauge != nil && model.Gauge.Value != nil {
			firstValue = model.Gauge.Value
		}
	}

	if firstValue != nil {
		return *firstValue
	}

	return 0.0
}

// RateFromCounter can be used to extract the instante rate of a Prometheus Metric object.
// Right now, the computation is simply to take the diff between two interval checks and
// infer the rate from there.
//
// One caveats of using instant rate like that is that if you have a process that logs
// actual rate each 30s and your rate interval is 1s, then in cases rate changed only
// a few times during the 30s, you have good chance of getting a report of 0 element/<interval>.
// This is because when you log, you hit an instance where the value did not change.
//
// *Important* This for now handles `Vec` metrics by summing for all labels, extracting
// for one specific label or for all labels is not yet supported.
type RateFromCounter struct {
	counter       prometheus.Collector
	interval      time.Duration
	unit          string
	previousTotal uint64
	actualTotal   uint64

	isAverage bool
}

func NewPerSecondRateFromCounter(counter *dmetrics.CounterVec, unit string) *RateFromCounter {
	return NewRateFromCounter(counter, 1*time.Second, unit)
}

func NewPerMinuteRateFromCounter(counter *dmetrics.CounterVec, unit string) *RateFromCounter {
	return NewRateFromCounter(counter, 1*time.Minute, unit)
}

// NewRateFromCounter creates a counter on which it's easy to how many time an event happen over a fixed
// period of time.
//
// For example, if over 1 second you process 20 blocks, then querying the counter within this 1s interval
// will yield a result of 20 blocks/s. The rate change as the time moves.
//
// ```
// counter := NewRateFromCounter(1*time.Second, "s", "blocks")
// counter.IncByElapsed(since1)
// counter.IncByElapsed(since2)
// counter.IncByElapsed(since3)
//
// counter.String() == ~150ms/block (over 1s)
// ```
func NewRateFromCounter(counter *dmetrics.CounterVec, interval time.Duration, unit string) *RateFromCounter {
	rate := &RateFromCounter{counter, interval, unit, 0, 0, false}

	// FIXME: See `run` documentation about the FIXME
	rate.run()

	return rate
}

func (c *RateFromCounter) Total() uint64 {
	return c.actualTotal
}

func (c *RateFromCounter) total() uint64 {
	metricChan := make(chan prometheus.Metric, 16)
	go func() {
		c.counter.Collect(metricChan)
		close(metricChan)
	}()

	sum := 0.0
	for value := range metricChan {
		model := new(dto.Metric)
		err := value.Write(model)
		if err != nil {
			panic(err)
		}

		if model.Counter != nil && model.Counter.Value != nil {
			sum += *model.Counter.Value
		}
	}

	return uint64(sum)
}

func (c *RateFromCounter) RateInt64() int64 {
	return int64(c.rate())
}

func (c *RateFromCounter) RateFloat64() float64 {
	return c.rate()
}

func (c *RateFromCounter) RateString() string {
	if !c.isAverage {
		return strconv.FormatInt(c.RateInt64(), 10)
	}

	return strconv.FormatFloat(c.RateFloat64(), 'f', -1, 64)
}

func (c *RateFromCounter) rate() float64 {
	if c.actualTotal < c.previousTotal {
		return 0
	}

	return float64(c.actualTotal - c.previousTotal)
}

// FIXME: Use finalizer trick (search online) to stop the goroutine when the counter goes out of scope
// for now, lifecycle is not handled a rate from counter lives forever
func (c *RateFromCounter) run() {
	go func() {
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()

		for {
			<-ticker.C

			total := c.total()

			c.previousTotal = c.actualTotal
			c.actualTotal = total
		}
	}()
}

var ratioUnitRegex = regexp.MustCompile("^[^/]+/.+$")
var elapsedPerElementUnitPrefixRegex = regexp.MustCompile("^(h|min|s|ms)/")

func (c *RateFromCounter) String() string {
	// We perform special handling of ratio elemnt with and time elapsed per in particular
	// unit like `100 bytes/msg` or `150ms/block`.
	isRatioUnit := ratioUnitRegex.MatchString(c.unit)
	isElapsedPerElementUnit := isRatioUnit && elapsedPerElementUnitPrefixRegex.MatchString(c.unit)

	if c.isAverage {
		if isRatioUnit {
			template := "%s "
			if isElapsedPerElementUnit {
				template = "%s"
			}

			return fmt.Sprintf(template+"%s (over %s)", c.RateString(), c.unit, c.intervalString())
		}

		return fmt.Sprintf("%s %s/%s (%d total)", c.RateString(), c.unit, c.timeUnit(), c.actualTotal)
	}

	return fmt.Sprintf("%s %s/%s (%d total)", c.RateString(), c.unit, c.timeUnit(), c.actualTotal)
}

func (c *RateFromCounter) timeUnit() string {
	switch c.interval {
	case 1 * time.Second:
		return "s"
	case 1 * time.Minute:
		return "min"
	case 1 * time.Millisecond:
		return "ms"
	default:
		return c.interval.String()
	}
}

func (c *RateFromCounter) intervalString() string {
	switch c.interval {
	case 1 * time.Second:
		return "1s"
	case 1 * time.Minute:
		return "1min"
	case 1 * time.Millisecond:
		return "1ms"
	default:
		return c.interval.String()
	}
}
