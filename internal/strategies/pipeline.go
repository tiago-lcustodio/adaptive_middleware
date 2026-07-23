package strategies

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"adaptive-middleware/internal/monitor"
	"adaptive-middleware/internal/network"
)

// PipelineMessage estrutura como a mensagem persistida em disco será lida
type PipelineMessage struct {
	ID        string `json:"id"`
	Topic     string `json:"topic"`
	Payload   []byte `json:"payload"`
	Timestamp int64  `json:"timestamp"`
}

// StagedPipeline gerencia o armazenamento local temporário em disco durante Outages
type StagedPipeline struct {
	storageDir string
	downstream *network.DownstreamClient
	metrics    *monitor.SystemMetrics
}

// NewStagedPipeline inicializa e garante a existência da pasta de persistência local
func NewStagedPipeline(downstream *network.DownstreamClient, metrics *monitor.SystemMetrics) *StagedPipeline {
	dir := ".storage_pipeline"
	_ = os.MkdirAll(dir, 0755) // Cria a pasta oculta se não existir

	return &StagedPipeline{
		storageDir: dir,
		downstream: downstream,
		metrics:    metrics,
	}
}

// SaveToDisk grava o pacote IoT no sistema de arquivos para evitar perda de dados
func (sp *StagedPipeline) SaveToDisk(id string, topic string, payload []byte) error {
	pMsg := PipelineMessage{
		ID:        id,
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now().UnixNano(),
	}

	bytes, err := json.Marshal(pMsg)
	if err != nil {
		return fmt.Errorf("erro ao serializar mensagem: %v", err)
	}

	// Cria um arquivo único baseado no ID e timestamp para evitar colisões
	filename := fmt.Sprintf("msg_%d_%s.json", pMsg.Timestamp, id)
	filePath := filepath.Join(sp.storageDir, filename)

	err = os.WriteFile(filePath, bytes, 0644)
	if err != nil {
		return fmt.Errorf("erro ao escrever no disco: %v", err)
	}

	// ATUALIZAÇÃO DO DISCO: Incrementa o medidor do Prometheus a cada arquivo gerado
	monitor.DiskBufferGauge.Inc()

	fmt.Printf("[ESTRATÉGIA] [PIPELINE] Mensagem %s custodiada em disco com sucesso.\n", id)
	return nil
}

// FlushDisk tenta esvaziar a pasta local reenviando os dados armazenados para o Receiver
// Esta função será chamada automaticamente pelo despachante ou quando houver janelas operacionais
// FlushDisk tenta esvaziar a pasta local reenviando os dados armazenados para o Receiver
// Esta função será chamada automaticamente pelo despachante ou quando houver janelas operacionais
func (sp *StagedPipeline) FlushDisk() {
	if !sp.downstream.IsConnected() {
		return
	}

	files, err := os.ReadDir(sp.storageDir)
	if err != nil || len(files) == 0 {
		return
	}

	fmt.Printf("[ESTRATÉGIA] [PIPELINE] Tentando descarregar %d mensagens acumuladas em disco...\n", len(files))

	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		filePath := filepath.Join(sp.storageDir, file.Name())
		bytes, err := os.ReadFile(filePath)
		if err != nil {
			continue
		}

		var pMsg PipelineMessage
		if err := json.Unmarshal(bytes, &pMsg); err != nil {
			_ = os.Remove(filePath)
			monitor.DiskBufferGauge.Dec()
			continue
		}

		tempoEmDisco := time.Duration(time.Now().UnixNano() - pMsg.Timestamp)

		// Tenta reenviar a mensagem
		err = sp.downstream.PublishMessage("unioeste/iot/receiver", pMsg.Payload)
		if err == nil {
			_ = os.Remove(filePath)

			// 1. Alimenta os dados de entrega de sucesso na memória do Go
			sp.metrics.RecordDelivery(tempoEmDisco, true)

			// 2. INCREMENTA O CONTADOR DO PROMETHEUS (Crucial para o Grafana!)
			monitor.ProcessedMessagesCounter.WithLabelValues("OUTAGE", "sucesso").Inc()

			// 3. Atualiza o Gauge do disco
			monitor.DiskBufferGauge.Dec()

			fmt.Printf("[ESTRATÉGIA] [PIPELINE] Mensagem %s descarregada e limpa do disco.\n", pMsg.ID)

			// Pequeno intervalo para permitir a coleta pelo Prometheus/Grafana sem engasgar
			time.Sleep(20 * time.Millisecond)
		} else {
			break
		}
	}
}
