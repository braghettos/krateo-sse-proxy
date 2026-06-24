// Structured logging for the SSE proxy.
//
// We replace the stdlib log.Printf("[poller] ...") calls with slog using a JSON
// handler set as the process default — parity with authn/snowplow, which emit
// structured JSON logs. This is a non-gated improvement: it is always on (there
// is no pre-existing log-level/format flag in this service to honour), but it
// is minimal-risk — same call sites, same information, just structured.
//
// When a span is active on the supplied context (i.e. tracing is enabled and
// the request/poll is being traced), trace_id and span_id are attached so logs
// correlate with traces. When tracing is off the SpanContext is invalid and no
// trace fields are emitted.
package main

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// initLogging installs a JSON slog handler as the default logger. Level is read
// from LOG_LEVEL (debug|info|warn|error), defaulting to info.
func initLogging() {
	level := slog.LevelInfo
	switch os.Getenv("LOG_LEVEL") {
	case "debug", "DEBUG":
		level = slog.LevelDebug
	case "warn", "WARN":
		level = slog.LevelWarn
	case "error", "ERROR":
		level = slog.LevelError
	}
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))
}

// traceAttrs returns trace_id/span_id slog attributes when ctx carries a valid
// (sampled or not) span context, else nil. Cheap and safe when tracing is off.
func traceAttrs(ctx context.Context) []any {
	if ctx == nil {
		return nil
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return nil
	}
	return []any{
		slog.String("trace_id", sc.TraceID().String()),
		slog.String("span_id", sc.SpanID().String()),
	}
}

// slogInfoCtx logs at info with component + trace correlation.
func slogInfoCtx(ctx context.Context, component, msg string, attrs ...any) {
	slog.LogAttrs(ctx, slog.LevelInfo, msg, mergeAttrs(ctx, component, attrs)...)
}

// slogWarnCtx logs at warn with component + trace correlation.
func slogWarnCtx(ctx context.Context, component, msg string, attrs ...any) {
	slog.LogAttrs(ctx, slog.LevelWarn, msg, mergeAttrs(ctx, component, attrs)...)
}

// slogErrorCtx logs at error with component, the error, and trace correlation.
func slogErrorCtx(ctx context.Context, component, msg string, err error, attrs ...any) {
	all := attrs
	if err != nil {
		all = append(all, slog.Any("error", err))
	}
	slog.LogAttrs(ctx, slog.LevelError, msg, mergeAttrs(ctx, component, all)...)
}

// slogError is a context-less error log (startup/telemetry init paths).
func slogError(component, msg string, err error) {
	slogErrorCtx(context.Background(), component, msg, err)
}

// slogInfo is a context-less info log (startup paths).
func slogInfo(component, msg string, attrs ...any) {
	slogInfoCtx(context.Background(), component, msg, attrs...)
}

// mergeAttrs converts the variadic key/value attrs into a slog.Attr slice,
// prepending the component and any trace fields.
func mergeAttrs(ctx context.Context, component string, attrs []any) []slog.Attr {
	out := make([]slog.Attr, 0, len(attrs)+3)
	out = append(out, slog.String("component", component))
	for _, a := range traceAttrs(ctx) {
		if at, ok := a.(slog.Attr); ok {
			out = append(out, at)
		}
	}
	for _, a := range attrs {
		if at, ok := a.(slog.Attr); ok {
			out = append(out, at)
		}
	}
	return out
}
