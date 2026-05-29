package analyzer

import (
	"fmt"
	"time"

	"adaptive-middleware/internal/config"
	"adaptive-middleware/internal/monitor"
)

type Engine struct {
	cfg          *config.Config
	metrics      *monitor.SystemMetrics
	stopChannel  chan struct{}
	cooldownTime time.Time
}

func NewEngine(cfg *config.Config, metrics *monitor.SystemMetrics) *Engine {
	return &Engine{
		cfg:         cfg,
		metrics:     metrics,
		stopChannel: make(chan struct{}),
	}
}

func (e *Engine) Start() {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		fmt.Println("[ANALYZER] Motor adaptativo MAPE-K iniciado com sucesso.")

		for {
			select {
			case <-ticker.C:
				e.analyze()
			case <-e.stopChannel:
				fmt.Println("[ANALYZER] Motor adaptativo encerrado.")
				return
			}
		}
	}()
}

func (e *Engine) Stop() {
	close(e.stopChannel)
}

func (e *Engine) analyze() {
	p95Latency, errorRate, consecutiveErrors, ingressRate := e.metrics.Snapshot()
	currentState := e.metrics.GetState()

	var nextState monitor.MiddlewareState = monitor.StateNormal

	if consecutiveErrors >= 5 {
		nextState = monitor.StateOutage
	} else if p95Latency > e.cfg.SlowConsumerThreshold && errorRate < 0.10 {
		nextState = monitor.StateSlowConsumer
	} else if errorRate > e.cfg.LossyLinkErrorRate && p95Latency <= e.cfg.SlowConsumerThreshold && p95Latency > 0 {
		nextState = monitor.StateLossyLink
	} else if ingressRate > e.cfg.TrafficSpikeThreshold {
		nextState = monitor.StateTrafficSpike
	} else if errorRate > 0.05 && p95Latency > (e.cfg.SlowConsumerThreshold/2) {
		nextState = monitor.StateFlapping
	}

	if nextState == monitor.StateNormal && currentState != monitor.StateNormal {
		if e.cooldownTime.IsZero() {
			e.cooldownTime = time.Now().Add(e.cfg.CooldownPeriod)
			nextState = currentState
			fmt.Printf("[ANALYZER] Sintomas de normalização detectados. Aguardando estabilização até: %v\n", e.cooldownTime.Format("15:04:05"))
		} else if time.Now().Before(e.cooldownTime) {
			nextState = currentState
		} else {
			e.cooldownTime = time.Time{}
			fmt.Println("[ANALYZER] >> SISTEMA REESTABELECIDO: Retornando ao comportamento operacional NORMAL.")
		}
	} else if nextState != monitor.StateNormal {
		e.cooldownTime = time.Time{}
	}

	if nextState != currentState {
		fmt.Printf("[ANALYZER] CHAVEAMENTO DE ESTRATÉGIA: Alterando de [%s] para ===> [%s]\n", currentState, nextState)
		fmt.Printf("           [Métricas Auxiliares] P95 Latency: %v | Error Rate: %.2f%% | Ingress Rate: %d msgs/s\n", p95Latency, errorRate*100, ingressRate)

		e.metrics.SetState(nextState)
	}
}
