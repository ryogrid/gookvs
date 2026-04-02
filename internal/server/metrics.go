package server

import (
	"github.com/prometheus/client_golang/prometheus"
)

// defaultDurationBuckets provides histogram buckets for duration metrics.
// Range: 100µs – 52s (factor 2.0, 20 buckets).
// Defined as a literal to avoid ExponentialBuckets signature compatibility issues.
var defaultDurationBuckets = []float64{
	0.0001, 0.0002, 0.0004, 0.0008, 0.0016, 0.0032, 0.0064, 0.0128,
	0.0256, 0.0512, 0.1024, 0.2048, 0.4096, 0.8192, 1.6384, 3.2768,
	6.5536, 13.1072, 26.2144, 52.4288,
}

// --- gRPC request metrics ---

var grpcMsgDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "gookv",
		Subsystem: "grpc",
		Name:      "msg_duration_seconds",
		Help:      "Duration of gRPC messages.",
		Buckets:   defaultDurationBuckets,
	},
	[]string{"type"},
)

var grpcMsgTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "gookv",
		Subsystem: "grpc",
		Name:      "msg_total",
		Help:      "Total number of gRPC messages.",
	},
	[]string{"type"},
)

var grpcMsgFailTotal = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "gookv",
		Subsystem: "grpc",
		Name:      "msg_fail_total",
		Help:      "Total number of failed gRPC messages.",
	},
	[]string{"type"},
)

// --- ReadIndex & Propose metrics ---

var readindexDuration = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: "gookv",
		Name:      "readindex_duration_seconds",
		Help:      "Duration of ReadIndex round-trips.",
		Buckets:   defaultDurationBuckets,
	},
)

var readindexBatchSize = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: "gookv",
		Name:      "readindex_batch_size",
		Help:      "Number of waiters per ReadIndex batch.",
		Buckets:   prometheus.LinearBuckets(1, 4, 16), // 1, 5, 9, ..., 61
	},
)

var proposeDuration = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: "gookv",
		Name:      "propose_duration_seconds",
		Help:      "Duration of Raft propose round-trips.",
		Buckets:   defaultDurationBuckets,
	},
)

// --- Storage command metrics ---

var storageCommandDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "gookv",
		Subsystem: "storage",
		Name:      "command_duration_seconds",
		Help:      "Duration of storage commands.",
		Buckets:   defaultDurationBuckets,
	},
	[]string{"type"},
)

var storageApplyBatchSize = prometheus.NewHistogram(
	prometheus.HistogramOpts{
		Namespace: "gookv",
		Subsystem: "storage",
		Name:      "apply_batch_size",
		Help:      "Number of modifies per ApplyModifies batch.",
		Buckets:   prometheus.LinearBuckets(1, 4, 16),
	},
)

var leaseReadTotal = prometheus.NewCounter(
	prometheus.CounterOpts{
		Namespace: "gookv",
		Name:      "lease_read_total",
		Help:      "Total reads served via leader lease (bypassing ReadIndex).",
	},
)

func init() {
	prometheus.MustRegister(grpcMsgDuration)
	prometheus.MustRegister(grpcMsgTotal)
	prometheus.MustRegister(grpcMsgFailTotal)
	prometheus.MustRegister(readindexDuration)
	prometheus.MustRegister(readindexBatchSize)
	prometheus.MustRegister(proposeDuration)
	prometheus.MustRegister(storageCommandDuration)
	prometheus.MustRegister(storageApplyBatchSize)
	prometheus.MustRegister(leaseReadTotal)
}
