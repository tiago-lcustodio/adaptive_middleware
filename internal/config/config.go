package config

import (
	"os"
	"time"
)

// Config segura todas as variáveis globais de ambiente e thresholds
type Config struct {
	// Endereços de Rede e Conexões Reais
	TTSEndpoint string // Host do Broker do TTS (Ex: "tcp://eu1.thethings.network:1883")
	TTSUser     string // Application ID do seu console TTS
	TTSPassword string // API Key gerada no console TTS
	TTSTopic    string // Tópico de Upstream (Ex: "v3/+/devices/+/up")

	ReceiverEndpoint string // Endereço do seu Receiver Python (Ex: "tcp://localhost:1884")
	PrometheusPort   string // Porta HTTP onde o Prometheus vai buscar as métricas

	// Thresholds de Falha (Limiares acadêmicos)
	SlowConsumerThreshold time.Duration
	LossyLinkErrorRate    float64
	TrafficSpikeThreshold int

	// Tempo de normalização
	CooldownPeriod time.Duration
}

// LoadConfig inicializa os valores buscando do ambiente ou aplicando defaults
func LoadConfig() *Config {
	return &Config{
		TTSEndpoint:           getEnv("TTS_ENDPOINT", "tcp://eu1.cloud.thethings.network:1883"),
		TTSUser:               getEnv("TTS_USER", "custodiotiago"),
		TTSPassword:           getEnv("TTS_PASSWORD", "NNSXS.YI4HFKJAME7G64SV4DL74KKAMZDMWXFEUJ4QB5Y.CBQDXBFJM4WQ6WMAMAU5TLHBM7GP4J3P2KXRBR3JB5SVATKNJKRA"),
		TTSTopic:              getEnv("TTS_TOPIC", "v3/custodiotiago@ttn/devices/+/up"),
		ReceiverEndpoint:      getEnv("RECEIVER_ENDPOINT", "tcp://localhost:1884"),
		PrometheusPort:        getEnv("PROMETHEUS_PORT", ":8082"),
		SlowConsumerThreshold: 2 * time.Second,
		LossyLinkErrorRate:    0.15,
		TrafficSpikeThreshold: 100,
		CooldownPeriod:        15 * time.Second,
	}
}

// Função auxiliar para capturar variável de ambiente com valor padrão se não existir
func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
