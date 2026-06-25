// OpenTelemetry wiring for the SSE proxy.
//
// Everything here is GATED and DEFAULT-OFF. The master gate is OTEL_ENABLED;
// OTEL_TRACING_ENABLED and OTEL_METRICS_ENABLED each default to OTEL_ENABLED
// when unset and override it when set. With the master off and neither
// per-signal flag set to a truthy value, Setup registers NOTHING: no
// exporters, no global TracerProvider/MeterProvider, no propagator. In that
// state the otelhttp handler/transport wrappers (also gated by the same flags
// at their call sites) are never installed, so the binary behaves byte-for-byte
// as it did before this package existed — the no-op global providers the otel
// API ships with are used and emit nothing.
//
// When enabled, traces and/or metrics are exported over OTLP/HTTP to
// OTEL_EXPORTER_OTLP_ENDPOINT, the resource is service.name=sse-proxy, and the
// global propagator is W3C TraceContext + Baggage so inbound `traceparent`
// headers from the browser are extracted and outbound poller requests carry it.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

const (
	// serviceName is the OTel resource service.name for this process.
	serviceName = "sse-proxy"

	// envEnabledMaster is the master gate for all OTel signals. It is the
	// default for the per-signal flags below: when OTEL_ENABLED is truthy and a
	// per-signal flag is unset, that signal is on. A per-signal flag, when set,
	// always wins over the master. Default-OFF: unset ⇒ everything off.
	envEnabledMaster  = "OTEL_ENABLED"
	envTracingEnabled = "OTEL_TRACING_ENABLED"
	envMetricsEnabled = "OTEL_METRICS_ENABLED"
)

// telemetry holds the gating decisions resolved at startup so the rest of the
// process can ask "is tracing on?" / "is metrics on?" without re-reading env.
type telemetry struct {
	tracingEnabled bool
	metricsEnabled bool
}

// logStatus emits a one-line summary of the OTel posture at startup.
func (t telemetry) logStatus() {
	slogInfo("telemetry", "otel status",
		slog.Bool("enabled", envEnabled(envEnabledMaster)),
		slog.Bool("tracing_enabled", t.tracingEnabled),
		slog.Bool("metrics_enabled", t.metricsEnabled),
		slog.String("endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
	)
}

// envEnabled reports whether the given env var is set to a truthy value.
// Unset/empty/unparseable ⇒ false (default-OFF).
func envEnabled(key string) bool {
	v, err := strconv.ParseBool(os.Getenv(key))
	return err == nil && v
}

// envEnabledOr reports whether key is truthy, defaulting to fallback when key
// is unset/empty. A set-but-unparseable value is treated as false (off). This
// is how a per-signal flag (OTEL_TRACING_ENABLED / OTEL_METRICS_ENABLED) takes
// its default from the master (OTEL_ENABLED) yet still wins when set.
func envEnabledOr(key string, fallback bool) bool {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	return err == nil && v
}

// Setup resolves the gating flags and, when enabled, installs the global OTel
// TracerProvider / MeterProvider and the W3C propagator. The returned shutdown
// func flushes and stops whatever was installed (a no-op when nothing was).
func setupTelemetry(ctx context.Context) (telemetry, func(context.Context) error) {
	// Master gate (default-OFF) is the default for each per-signal flag; a
	// per-signal flag, when set, overrides the master.
	master := envEnabled(envEnabledMaster)
	t := telemetry{
		tracingEnabled: envEnabledOr(envTracingEnabled, master),
		metricsEnabled: envEnabledOr(envMetricsEnabled, master),
	}

	// Nothing enabled ⇒ register nothing; keep the otel no-op globals.
	if !t.tracingEnabled && !t.metricsEnabled {
		return t, func(context.Context) error { return nil }
	}

	var shutdowns []func(context.Context) error
	shutdown := func(ctx context.Context) error {
		var err error
		for _, fn := range shutdowns {
			err = errors.Join(err, fn(ctx))
		}
		return err
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		// Fall back to a minimal resource rather than failing startup.
		res = resource.NewSchemaless(semconv.ServiceName(serviceName))
	}

	// W3C TraceContext + Baggage propagation so inbound traceparent headers are
	// extracted and outbound requests carry context. Set whenever either signal
	// is on (harmless for metrics-only, required for tracing).
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	if t.tracingEnabled {
		// Endpoint is taken from OTEL_EXPORTER_OTLP_ENDPOINT (and the standard
		// OTEL_EXPORTER_OTLP_* env) by the exporter itself.
		texp, err := otlptracehttp.New(ctx)
		if err != nil {
			slogError("telemetry", "otlp trace exporter init failed", err)
			t.tracingEnabled = false
		} else {
			tp := sdktrace.NewTracerProvider(
				sdktrace.WithBatcher(texp),
				sdktrace.WithResource(res),
			)
			otel.SetTracerProvider(tp)
			shutdowns = append(shutdowns, tp.Shutdown)
		}
	}

	if t.metricsEnabled {
		mexp, err := otlpmetrichttp.New(ctx)
		if err != nil {
			slogError("telemetry", "otlp metric exporter init failed", err)
			t.metricsEnabled = false
		} else {
			mp := sdkmetric.NewMeterProvider(
				sdkmetric.WithReader(sdkmetric.NewPeriodicReader(mexp)),
				sdkmetric.WithResource(res),
			)
			otel.SetMeterProvider(mp)
			shutdowns = append(shutdowns, mp.Shutdown)
		}
	}

	return t, shutdown
}
