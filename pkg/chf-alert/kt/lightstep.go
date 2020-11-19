package kt

import (
	"context"
	"os"
	"time"

	"github.com/kentik/chf-alert/pkg/version"
	"github.com/kentik/golog/logger"
	"github.com/lightstep/lightstep-tracer-go"
	"github.com/opentracing/opentracing-go"
)

type LightstepConfig struct {
	EnableLightStepTracing bool
	LightStepToken         string
	LightStepHost          string
	LightStepPort          int
}

func GetLightstepConfig() LightstepConfig {
	return LightstepConfig{
		EnableLightStepTracing: ParseBoolDefaultOrPanic(os.Getenv(EnableLightStepTracingEnvVar), EnableLightStepTracingDefault),
		LightStepToken:         StrOrDef(os.Getenv(LightStepTracingTokenEnvVar), LightStepTracingTokenDefault),
		LightStepHost:          StrOrDef(os.Getenv(LightstepTracingHostEnvVar), LightstepTracingHostDefault),
		LightStepPort:          AtoiDefaultOrPanic(os.Getenv(LightstepTracingPortEnvVar), LightstepTracingPortDefault),
	}
}

func InitLightstep(logger *logger.Logger, config LightstepConfig, service string) func() {
	logger.Debugf("lightstep", "Setting up lightstep + opentracing")
	lightstepTracer := lightstep.NewTracer(lightstep.Options{
		Collector: lightstep.Endpoint{
			Host:      config.LightStepHost,
			Port:      config.LightStepPort,
			Plaintext: true,
		},
		UseHttp:     false,
		UseGRPC:     true,
		AccessToken: config.LightStepToken,
		Propagators: map[opentracing.BuiltinFormat]lightstep.Propagator{
			opentracing.HTTPHeaders: lightstep.B3Propagator,
			opentracing.TextMap:     lightstep.B3Propagator,
		},
		Tags: map[string]interface{}{
			lightstep.ComponentNameKey: "alert",
			"alerting_svc":             service,
			"service.version":          version.VERSION_STRING,
		},
		MaxLogValueLen: 4096,
	})
	noopCloser := func() {}

	if lightstepTracer == nil {
		logger.Errorf("lightstep", "Tracing disabled: could not create lightstep tracer")
		return noopCloser
	}
	opentracing.SetGlobalTracer(lightstepTracer)
	lightstep.SetGlobalEventHandler(func(event lightstep.Event) { /* noop */ })

	logger.Infof("lightstep", "Tracing enabled: lightstep")
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		logger.Infof("lightstep", "Tracing shutting down: lightstep")
		lightstepTracer.Close(ctx)
		cancel()
	}
}
