package main

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"adaptive-middleware/internal/analyzer"
	"adaptive-middleware/internal/config"
	"adaptive-middleware/internal/monitor"
	"adaptive-middleware/internal/network"
	"adaptive-middleware/internal/router"
	"adaptive-middleware/internal/strategies"
)

func main() {
	fmt.Println("==================================================================")
	fmt.Println("     INICIALIZANDO MIDDLEWARE RESILIENTE ADAPTATIVO (UNIOESTE)    ")
	fmt.Println("==================================================================")

	cfg := config.LoadConfig()
	metrics := monitor.NewSystemMetrics()
	exporter := monitor.NewExporter(cfg, metrics)
	engine := analyzer.NewEngine(cfg, metrics)

	downstreamClient := network.NewDownstreamClient(cfg, metrics)
	if err := downstreamClient.Connect(); err != nil {
		fmt.Printf("[SYSTEM] [AVISO] Receiver offline na inicialização: %v\n", err)
	}

	pipelineStrategy := strategies.NewStagedPipeline(downstreamClient)
	resilienceStrategy := strategies.NewResilienceStrategies(downstreamClient)
	// NOVO: Instancia o módulo das estratégias de tráfego (Throttling / Ring Eviction)
	trafficStrategy := strategies.NewTrafficStrategies(downstreamClient)

	// ATUALIZADO: Passa o trafficStrategy para o construtor do Dispatcher
	dispatcher := router.NewDispatcher(cfg, metrics, downstreamClient, pipelineStrategy, resilienceStrategy, trafficStrategy)
	upstreamListener := router.NewTTSListener(cfg, dispatcher)

	exporter.Start()
	dispatcher.Start()
	engine.Start()

	if err := upstreamListener.Start(); err != nil {
		fmt.Printf("[SYSTEM] [AVISO] Não foi possível conectar ao Broker MQTT: %v\n", err)
		fmt.Println("[SYSTEM] Ativando fallback: Mantendo simulação acadêmica ligada.")

		stopSimulation := make(chan struct{})
		go runAcademicSimulation(dispatcher, metrics, stopSimulation)
		defer close(stopSimulation)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	fmt.Println("\n[SYSTEM] Sinal de encerramento recebido. Desligando componentes...")

	upstreamListener.Stop()
	engine.Stop()
	dispatcher.Stop()
	downstreamClient.Disconnect()
	exporter.Stop()

	fmt.Println("==================================================================")
	fmt.Println("             MIDDLEWARE ENCERRADO COM SUCESSO (LIMPO)             ")
	fmt.Println("==================================================================")
}

func runAcademicSimulation(d *router.Dispatcher, m *monitor.SystemMetrics, stop chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-ticker.C:
			elapsed := time.Since(startTime).Seconds()

			var latency time.Duration = 15 * time.Millisecond
			var success bool = true

			// MOTOR DE SINTOMAS CONTINUA ATIVO:
			// Força falhas artificiais na saída para obrigar o Analyzer a testar as estratégias
			if elapsed >= 15 && elapsed < 30 {
				success = false
				latency = 0
			} else if elapsed >= 40 && elapsed < 55 {
				latency = 2500 * time.Millisecond
				success = true
			} else if elapsed >= 65 && elapsed < 80 {
				if rand.Float64() < 0.35 {
					success = false
					latency = 0
				} else {
					success = true
					latency = 40 * time.Millisecond
				}
			}

			// NOTA: Removemos a geração de mensagens falsas e o d.IngestMessage daqui!
			// Quem vai alimentar o d.IngestMessage agora é EXCLUSIVAMENTE o teu listener.go com os dados do TTS!

			// Apenas reporta a telemetria do estado da rede de saída para o ciclo MAPE-K
			m.RecordDelivery(latency, success)

		case <-stop:
			return
		}
	}
}
