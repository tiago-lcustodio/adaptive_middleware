package network

import (
	"fmt"
	"time"

	"adaptive-middleware/internal/config"
	"adaptive-middleware/internal/monitor"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// DownstreamClient gerencia a conexão de saída em direção ao Receiver Python
type DownstreamClient struct {
	cfg     *config.Config
	metrics *monitor.SystemMetrics
	client  mqtt.Client
}

// NewDownstreamClient configura as opções de rede do barramento de saída
func NewDownstreamClient(cfg *config.Config, metrics *monitor.SystemMetrics) *DownstreamClient {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.ReceiverEndpoint) // Aponta para a porta do Receiver (Ex: localhost:1884)
	opts.SetClientID("adaptive_middleware_downstream")

	opts.SetAutoReconnect(true)
	opts.SetConnectRetryInterval(2 * time.Second)
	opts.SetCleanSession(true)

	return &DownstreamClient{
		cfg:     cfg,
		metrics: metrics,
		client:  mqtt.NewClient(opts),
	}
}

// Connect estabelece a ligação com o Broker de destino/Receiver
func (dc *DownstreamClient) Connect() error {
	fmt.Printf("[MQTT-DOWNSTREAM] Conectando ao barramento do Receiver em: %s...\n", dc.cfg.ReceiverEndpoint)
	if token := dc.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("erro ao conectar no downstream: %v", token.Error())
	}
	fmt.Println("[MQTT-DOWNSTREAM] Conexão com o destino estabelecida com sucesso.")
	return nil
}

// IsConnected checa o estado atual do link de rede
func (dc *DownstreamClient) IsConnected() bool {
	return dc.client != nil && dc.client.IsConnected()
}

// Disconnect encerra a sessão de rede de forma limpa
func (dc *DownstreamClient) Disconnect() {
	if dc.client != nil && dc.client.IsConnected() {
		fmt.Println("[MQTT-DOWNSTREAM] Desconectando do barramento de destino...")
		dc.client.Disconnect(250)
	}
}

// PublishMessage tenta entregar o pacote de dados IoT ao destino e mede o RTT (Round-Trip Time)
func (dc *DownstreamClient) PublishMessage(topic string, payload []byte) error {
	// 1. Marca o início exato do envio para cálculo de telemetria acadêmica
	startTime := time.Now()

	// Se a conexão física caiu, reporta falha imediata para o laço MAPE-K reagir
	if !dc.IsConnected() {
		dc.metrics.RecordDelivery(0, false)
		return fmt.Errorf("link downstream desconectado")
	}

	// 2. Dispara a publicação via MQTT com QoS 1 (Garante a necessidade de ACK)
	token := dc.client.Publish(topic, 1, false, payload)

	// Aguarda o ACK chegar ou estourar o threshold definido na dissertação
	// Usamos uma espera com limite de tempo baseada no seu Config
	timeoutReached := !token.WaitTimeout(dc.cfg.SlowConsumerThreshold + (500 * time.Millisecond))

	// 3. Calcula a latência exata que levou a operação de rede
	elapsed := time.Since(startTime)

	if timeoutReached {
		// Cenário de Slow Consumer agressivo ou travamento: registramos o estouro no Monitor
		dc.metrics.RecordDelivery(elapsed, false)
		return fmt.Errorf("timeout estourado aguardando ACK do receiver")
	}

	if token.Error() != nil {
		// Cenário de link instável/erro de socket: registra falha no Monitor
		dc.metrics.RecordDelivery(elapsed, false)
		return token.Error()
	}

	// 4. SUCESSO TOTAL: Mensagem entregue e ACK recebido dentro do prazo legal!
	// Alimenta o Monitor com os dados reais para cálculo do P95 Latency
	dc.metrics.RecordDelivery(elapsed, true)
	return nil
}
