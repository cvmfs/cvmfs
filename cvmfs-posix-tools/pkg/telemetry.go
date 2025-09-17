package pkg

import (
	"context"
	"os"
	"time"

	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
)

const OTLP_TRACES_DEFAULT = "http://localhost:4318"

func InitTracer(ctx context.Context, serviceName string, serviceVersion string) (func(context.Context) error, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	otel.SetTextMapPropagator(newPropagator(ctx))

	traceProvider, err := newTraceProvider(ctx, serviceName, serviceVersion)
	if err != nil {
		return nil, err
	}

	otel.SetTracerProvider(traceProvider)

	return traceProvider.Shutdown, nil
}

func newPropagator(ctx context.Context) propagation.TextMapPropagator {
	propagator := propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)

	if traceparent := os.Getenv("TRACEPARENT"); traceparent != "" {
		carrier := propagation.MapCarrier{
			"traceparent": traceparent,
		}
		propagator.Inject(ctx, carrier)
		log.Info().Str("traceparent", carrier.Get("traceparent")).Msg("Injecting TRACEPARENT from env")
	}

	return propagator
}

func newTraceProvider(ctx context.Context, serviceName string, serviceVersion string) (*trace.TracerProvider, error) {
	resource, err := resource.New(
		ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithProcess(),
		resource.WithOS(),
		resource.WithContainer(),
		resource.WithHost(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(serviceVersion),
		),
	)
	if err != nil {
		return nil, err
	}

	var traceExporter *otlptrace.Exporter
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") == "" {
		traceExporter, err = otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(OTLP_TRACES_DEFAULT))
	} else {
		traceExporter, err = otlptracehttp.New(ctx)
	}
	if err != nil {
		log.Error().Err(err).Msg("Error creating trace server.")
		return nil, err
	}

	return trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(resource),
	), nil
}
