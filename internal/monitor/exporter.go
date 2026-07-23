package monitor

import (
	"fmt"
	"net/http"
	"time"

	"adaptive-middleware/internal/config"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	ActiveStrategyGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "middleware_active_strategy",
			Help: "Estratégia ativa atual: 0=NORMAL, 1=OUTAGE, 2=SLOW_CONSUMER, 3=FLAPPING, 4=LOSSY_LINK, 5=TRAFFIC_SPIKE",
		},
		[]string{"state"},
	)

	ProcessedMessagesCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "middleware_processed_messages_total",
			Help: "Total de mensagens IoT processadas pelo despachante do middleware",
		},
		[]string{"strategy", "status"},
	)

	// NOVO: Medidor da fila em memória (canal do Go)
	QueueSizeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "middleware_queue_size",
		Help: "Quantidade atual de mensagens aguardando processamento no canal interno.",
	})

	// NOVO: Medidor de acúmulo de arquivos físicos em disco do Pipeline
	DiskBufferGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "middleware_pipeline_disk_messages",
		Help: "Quantidade de mensagens persistidas em disco aguardando o Receiver voltar.",
	})

	// ADICIONADO: Medidor da latência P95 em segundos calculada na janela de amostragem
	DeliveryLatencyGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "middleware_delivery_latency_seconds",
		Help: "Latencia de entrega P95 calculada na ultima janela de amostragem.",
	})
)

type Exporter struct {
	cfg     *config.Config
	metrics *SystemMetrics
	server  *http.Server
}

func NewExporter(cfg *config.Config, metrics *SystemMetrics) *Exporter {
	prometheus.MustRegister(ActiveStrategyGauge)
	prometheus.MustRegister(ProcessedMessagesCounter)
	// NOVO: Registro das novas métricas de infraestrutura
	prometheus.MustRegister(QueueSizeGauge)
	prometheus.MustRegister(DiskBufferGauge)
	// ADICIONADO: Registro do medidor de latência P95
	prometheus.MustRegister(DeliveryLatencyGauge)

	ActiveStrategyGauge.WithLabelValues(string(StateNormal)).Set(0)
	return &Exporter{
		cfg:     cfg,
		metrics: metrics,
	}
}

func (e *Exporter) Start() {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	e.server = &http.Server{
		Addr:    e.cfg.PrometheusPort,
		Handler: mux,
	}

	go func() {
		fmt.Printf("[MONITOR] Servidor do Prometheus HTTP ativado em http://localhost%s/metrics\n", e.cfg.PrometheusPort)
		if err := e.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[MONITOR] [ERRO] Falha crítica ao iniciar servidor Prometheus: %v\n", err)
		}
	}()

	// Goroutine que atualiza os valores do Prometheus continuamente baseado na RAM do monitor
	go func() {
		for {
			state := e.metrics.GetState()
			var val float64 = 0
			switch state {
			case StateNormal:
				val = 0
			case StateOutage:
				val = 1
			case StateSlowConsumer:
				val = 2
			case StateFlapping:
				val = 3
			case StateLossyLink:
				val = 4
			case StateTrafficSpike:
				val = 5
			}

			// Limpa e atualiza o medidor
			ActiveStrategyGauge.Reset()
			ActiveStrategyGauge.WithLabelValues(string(state)).Set(val)
			time.Sleep(200 * time.Millisecond)
		}
	}()
}

func (e *Exporter) Stop() {
	if e.server != nil {
		fmt.Println("[MONITOR] Encerrando servidor HTTP do Prometheus...")
		e.server.Close()
	}
}
