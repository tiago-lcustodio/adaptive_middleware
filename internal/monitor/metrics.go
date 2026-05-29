package monitor

import (
	"sync"
	"time"
)

type MiddlewareState string

const (
	StateNormal       MiddlewareState = "NORMAL"
	StateOutage       MiddlewareState = "OUTAGE"
	StateSlowConsumer MiddlewareState = "SLOW_CONSUMER"
	StateFlapping     MiddlewareState = "FLAPPING"
	StateLossyLink    MiddlewareState = "LOSSY_LINK"
	StateTrafficSpike MiddlewareState = "TRAFFIC_SPIKE"
)

type SystemMetrics struct {
	mu              sync.RWMutex
	currentState    MiddlewareState
	latencies       []time.Duration
	totalSent       int64
	failedSent      int64
	consecutiveErr  int64
	ingressMessages int64
	lastWindowTime  time.Time
}

func NewSystemMetrics() *SystemMetrics {
	return &SystemMetrics{
		currentState:   StateNormal,
		lastWindowTime: time.Now(),
		latencies:      make([]time.Duration, 0, 1000),
	}
}

func (sm *SystemMetrics) SetState(state MiddlewareState) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// 1. Atualiza o estado interno na memória do Go
	sm.currentState = state

	// 2. Reseta o valor do estado anterior para 0 (opcional, para limpar o gráfico)
	// Se quiser que apenas uma estratégia fique com valor '1' por vez:
	ActiveStrategyGauge.Reset()

	// 3. Define o valor numérico correspondente à estratégia ativa
	var valor float64
	switch state {
	case StateNormal:
		valor = 0
	case StateOutage: // Aciona Staged Pipeline
		valor = 1
	case StateSlowConsumer: // Aciona Circuit Breaker
		valor = 2
	case StateFlapping: // Aciona Throttling
		valor = 3
	case StateLossyLink: // Aciona Replicação Ativa
		valor = 4
	case StateTrafficSpike: // Aciona Ring Buffer
		valor = 5
	default:
		valor = 0
	}

	// 4. Publica a métrica atualizada com o Rótulo Correto para o Prometheus coletar
	ActiveStrategyGauge.WithLabelValues(string(state)).Set(valor)
}

func (sm *SystemMetrics) GetState() MiddlewareState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.currentState
}

func (sm *SystemMetrics) RecordDelivery(latency time.Duration, success bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.totalSent++
	if success {
		sm.consecutiveErr = 0
		if len(sm.latencies) >= 100 {
			sm.latencies = sm.latencies[1:]
		}
		sm.latencies = append(sm.latencies, latency)
	} else {
		sm.failedSent++
		sm.consecutiveErr++
	}
}

func (sm *SystemMetrics) RecordIngress() {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.ingressMessages++
}

func (sm *SystemMetrics) Snapshot() (p95Latency time.Duration, errorRate float64, consecutiveErrors int64, ingressRate int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	p95Latency = 0
	if len(sm.latencies) > 0 {
		// Ordenação simples para percentil 95
		for i := 0; i < len(sm.latencies); i++ {
			for j := i + 1; j < len(sm.latencies); j++ {
				if sm.latencies[i] > sm.latencies[j] {
					sm.latencies[i], sm.latencies[j] = sm.latencies[j], sm.latencies[i]
				}
			}
		}
		p95Index := int(float64(len(sm.latencies)) * 0.95)
		if p95Index >= len(sm.latencies) {
			p95Index = len(sm.latencies) - 1
		}
		p95Latency = sm.latencies[p95Index]
	}

	errorRate = 0.0
	if sm.totalSent > 0 {
		errorRate = float64(sm.failedSent) / float64(sm.totalSent)
	}

	now := time.Now()
	duration := now.Sub(sm.lastWindowTime).Seconds()
	if duration > 0 {
		ingressRate = int(float64(sm.ingressMessages) / duration)
	}

	consecutiveErrors = sm.consecutiveErr
	sm.totalSent = 0
	sm.failedSent = 0
	sm.ingressMessages = 0
	sm.lastWindowTime = now

	return
}
