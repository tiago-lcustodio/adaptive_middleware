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
func (sp *StagedPipeline) FlushDisk() {
	if !sp.downstream.IsConnected() {
		return // Se o link de saída continuar caído, não gasta CPU tentando ler disco
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
			_ = os.Remove(filePath) // Remove arquivos corrompidos
			// ATUALIZAÇÃO DO DISCO: Decrementa o medidor do Prometheus ao descartar arquivo inválido
			monitor.DiskBufferGauge.Dec()
			continue
		}

		// AJUSTE DE QOS: Calcula o tempo em que a mensagem ficou retida em disco (Frescor do Dado)
		tempoEmDisco := time.Duration(time.Now().UnixNano() - pMsg.Timestamp)

		// Tenta re-enviar pelo canal oficial
		err = sp.downstream.PublishMessage("unioeste/iot/receiver", pMsg.Payload)
		if err == nil {
			// Se deu sucesso, apaga o arquivo do disco imediatamente
			_ = os.Remove(filePath)

			// Grava a telemetria do atraso acumulado para expor a latência de custódia real no P95
			sp.metrics.RecordDelivery(tempoEmDisco, true)

			// ATUALIZAÇÃO DO DISCO: Decrementa o medidor do Prometheus ao limpar arquivo enviado
			monitor.DiskBufferGauge.Dec()

			fmt.Printf("[ESTRATÉGIA] [PIPELINE] Mensagem %s descarregada e limpa do disco.\n", pMsg.ID)
			time.Sleep(10 * time.Millisecond) // Evita sobrecarga de vazão imediata (backpressure leve)
		} else {
			// Se falhou (ex: receptor caiu de novo no meio do flush), interrompe o ciclo para tentar mais tarde
			break
		}
	}
}
