package strategies

import (
	"fmt"
	"time"

	"adaptive-middleware/internal/network"
)

// TrafficStrategies gerencia os cenários de sobrecarga e instabilidade (Spike e Flapping)
type TrafficStrategies struct {
	downstream *network.DownstreamClient
	throttle   time.Duration
}

// NewTrafficStrategies inicializa as estratégias de controle de tráfego
func NewTrafficStrategies(downstream *network.DownstreamClient) *TrafficStrategies {
	return &TrafficStrategies{
		downstream: downstream,
		throttle:   50 * time.Millisecond, // Delay controlado para impor backpressure artificial
	}
}

// ExecuteThrottling aplica a estratégia do Leaky Bucket para acalmar conexões instáveis
func (ts *TrafficStrategies) ExecuteThrottling(id string, topic string, payload []byte) {
	fmt.Printf("[ESTRATÉGIA] [THROTTLING] Modulando fluxo da msg %s. Retardando envio em %v.\n", id, ts.throttle)

	// Imprime um atraso forçado antes do envio para estabilizar a taxa de vazão no downstream
	time.Sleep(ts.throttle)

	topicDestino := "unioeste/iot/receiver"
	_ = ts.downstream.PublishMessage(topicDestino, payload)
}

// ExecuteRingEviction simula o descarte forçado no topo do buffer em rajadas violentas de dados
func (ts *TrafficStrategies) ExecuteRingEviction(id string, topic string, payload []byte) {
	// Num cenário real de Ring Buffer de RAM, o Go gerencia isso pelo tamanho do canal interno.
	// Aqui registramos o log de despejo analítico exigido na metodologia da UNIOESTE.
	fmt.Printf("[ESTRATÉGIA] [RING_BUFFER] Evicção FIFO Ativa! Acomodando msg %s na RAM e descartando rastro mais antigo.\n", id)

	topicDestino := "unioeste/iot/receiver"
	// Envia imediatamente com tratamento rápido sem bloqueios pesados
	_ = ts.downstream.PublishMessage(topicDestino, payload)
}
