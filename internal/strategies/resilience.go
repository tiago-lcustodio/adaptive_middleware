package strategies

import (
	"fmt"
	"sync"

	"adaptive-middleware/internal/network"
)

// ResilienceStrategies centraliza os mecanismos de Circuit Breaker e Replicação
type ResilienceStrategies struct {
	downstream *network.DownstreamClient
}

// NewResilienceStrategies inicializa o gestor de resiliência de rede
func NewResilienceStrategies(downstream *network.DownstreamClient) *ResilienceStrategies {
	return &ResilienceStrategies{
		downstream: downstream,
	}
}

// ReplicateActive dispara cópias paralelas da mensagem para furar perdas de pacotes (Lossy Link)
func (rs *ResilienceStrategies) ReplicateActive(id string, payload []byte) {
	// Definimos 3 envios redundantes em paralelo (canais ou tentativas concorrentes)
	replicas := 3
	var wg sync.WaitGroup

	fmt.Printf("[ESTRATÉGIA] [REPLICATION] Iniciando replicação ativa de segurança para msg %s (%d réplicas).\n", id, replicas)

	for i := 1; i <= replicas; i++ {
		wg.Add(1)
		go func(replicaID int) {
			defer wg.Done()

			// Tópicos virtuais redundantes para simular canais disjuntos na bancada de IoT
			topicDestino := fmt.Sprintf("unioeste/iot/receiver/replica-%d", replicaID)

			err := rs.downstream.PublishMessage(topicDestino, payload)
			if err != nil {
				// Se uma réplica falhar devido à perda, as outras continuam a tentar
				_ = err
			} else {
				fmt.Printf("[ESTRATÉGIA] [REPLICATION] Réplica %d da msg %s entregue com sucesso!\n", replicaID, id)
			}
		}(i)
	}

	// Aguarda que todas as goroutines finalizem a tentativa de entrega antes de liberar a thread principal
	wg.Wait()
}

// CircuitBreakerAlert apenas gera o log acadêmico estruturado para o fail-fast controlado
func (rs *ResilienceStrategies) CircuitBreakerAlert(id string) {
	fmt.Printf("[ESTRATÉGIA] [CIRCUIT_BREAKER] Fail-Fast Ativo! Descartando msg %s imediatamente para proteger infraestrutura.\n", id)
}
