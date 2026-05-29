package router

import (
	"fmt"
	"time"

	"adaptive-middleware/internal/config"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// TTSListener gerencia a conexão de entrada (Upstream) vinda do The Things Stack
type TTSListener struct {
	cfg        *config.Config
	dispatcher *Dispatcher
	client     mqtt.Client
}

// NewTTSListener configura o cliente MQTT de entrada pronto para a rede
func NewTTSListener(cfg *config.Config, dispatcher *Dispatcher) *TTSListener {
	opts := mqtt.NewClientOptions()
	opts.AddBroker(cfg.TTSEndpoint)
	opts.SetClientID("adaptive_middleware_upstream")

	// Configura as credenciais caso o ambiente exija (The Things Stack necessita)
	if cfg.TTSUser != "" {
		opts.SetUsername(cfg.TTSUser)
		opts.SetPassword(cfg.TTSPassword)
	}

	// Propriedades de resiliência do próprio driver de rede
	opts.SetAutoReconnect(true)
	opts.SetConnectRetryInterval(2 * time.Second)
	opts.SetCleanSession(false) // Garante QoS se houver desconexão rápida

	return &TTSListener{
		cfg:        cfg,
		dispatcher: dispatcher,
		client:     mqtt.NewClient(opts),
	}
}

// Start estabelece a conexão com o broker e assina o tópico dos sensores
func (tl *TTSListener) Start() error {
	fmt.Printf("[MQTT-UPSTREAM] Conectando ao Broker do TTS em: %s...\n", tl.cfg.TTSEndpoint)

	if token := tl.client.Connect(); token.Wait() && token.Error() != nil {
		return fmt.Errorf("falha ao conectar no broker upstream: %v", token.Error())
	}

	// Define a função de callback que será disparada toda vez que uma mensagem chegar da rede
	messageHandler := func(client mqtt.Client, mqttMsg mqtt.Message) {
		// Encapsula o pacote de rede bruto na estrutura interna do middleware
		msg := Message{
			ID:        fmt.Sprintf("MSG-REAL-%d", time.Now().UnixNano()), // ID gerado por timestamp de alta precisão
			Topic:     mqttMsg.Topic(),
			Payload:   mqttMsg.Payload(),
			Timestamp: time.Now(),
		}

		// Envia para o Dispatcher processar de forma assíncrona
		tl.dispatcher.IngestMessage(msg)
	}

	// Subscreve no tópico do The Things Stack definido no config.go (QoS 1 garante entrega ao middleware)
	if token := tl.client.Subscribe(tl.cfg.TTSTopic, 1, messageHandler); token.Wait() && token.Error() != nil {
		return fmt.Errorf("falha ao subscrever no tópico TTS: %v", token.Error())
	}

	fmt.Printf("[MQTT-UPSTREAM] Ingestão ativa! Escutando tópico: %s\n", tl.cfg.TTSTopic)
	return nil
}

// Stop desconecta do Broker do TTS de forma limpa
func (tl *TTSListener) Stop() {
	if tl.client != nil && tl.client.IsConnected() {
		fmt.Println("[MQTT-UPSTREAM] Desconectando do broker de entrada...")
		tl.client.Disconnect(250)
	}
}
