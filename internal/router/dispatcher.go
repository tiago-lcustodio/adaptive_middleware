package router

import (
	"fmt"
	"sync"
	"time"

	"adaptive-middleware/internal/config"
	"adaptive-middleware/internal/monitor"
	"adaptive-middleware/internal/network"
	"adaptive-middleware/internal/strategies"
)

type Message struct {
	ID        string
	Topic     string
	Payload   []byte
	Timestamp time.Time
}

type Dispatcher struct {
	cfg         *config.Config
	metrics     *monitor.SystemMetrics
	downstream  *network.DownstreamClient
	pipeline    *strategies.StagedPipeline
	resilience  *strategies.ResilienceStrategies
	traffic     *strategies.TrafficStrategies // NOVO: Referência para controle de tráfego
	inputChan   chan Message
	stopChannel chan struct{}
	wg          sync.WaitGroup
}

func NewDispatcher(cfg *config.Config, metrics *monitor.SystemMetrics, downstream *network.DownstreamClient, pipeline *strategies.StagedPipeline, resilience *strategies.ResilienceStrategies, traffic *strategies.TrafficStrategies) *Dispatcher {
	return &Dispatcher{
		cfg:         cfg,
		metrics:     metrics,
		downstream:  downstream,
		pipeline:    pipeline,
		resilience:  resilience,
		traffic:     traffic, // Vincula o novo módulo
		inputChan:   make(chan Message, 5000),
		stopChannel: make(chan struct{}),
	}
}

func (d *Dispatcher) Start() {
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		fmt.Println("[ROUTER] Despachante dinâmico de tráfego iniciado.")

		flushTicker := time.NewTicker(2 * time.Second)
		defer flushTicker.Stop()

		for {
			select {
			case msg, open := <-d.inputChan:
				if !open {
					return
				}
				d.routeMessage(msg)
			case <-flushTicker.C:
				if d.metrics.GetState() == monitor.StateNormal {
					go d.pipeline.FlushDisk()
				}
			case <-d.stopChannel:
				fmt.Println("[ROUTER] Encerrando o despachante de tráfego...")
				return
			}
		}
	}()
}

func (d *Dispatcher) Stop() {
	close(d.stopChannel)
	d.wg.Wait()
}

func (d *Dispatcher) IngestMessage(msg Message) {
	d.metrics.RecordIngress()

	select {
	case d.inputChan <- msg:
	default:
		fmt.Printf("[ROUTER] [ALERTA] Buffer de entrada saturado! Descartando msg ID: %s\n", msg.ID)
		d.metrics.RecordDelivery(0, false)
	}
}

func (d *Dispatcher) routeMessage(msg Message) {
	currentState := d.metrics.GetState()

	switch currentState {
	case monitor.StateNormal:
		d.executeNormal(msg)
	case monitor.StateOutage:
		d.executeStagedPipeline(msg)
	case monitor.StateSlowConsumer:
		d.executeCircuitBreaker(msg)
	case monitor.StateFlapping:
		d.executeThrottling(msg)
	case monitor.StateLossyLink:
		d.executeActiveReplication(msg)
	case monitor.StateTrafficSpike:
		d.executeRingBufferEviction(msg)
	default:
		d.executeNormal(msg)
	}
}

func (d *Dispatcher) executeNormal(msg Message) {
	topicDestino := "unioeste/iot/receiver"
	err := d.downstream.PublishMessage(topicDestino, msg.Payload)
	if err != nil {
		_ = err
	}
}

func (d *Dispatcher) executeStagedPipeline(msg Message) {
	err := d.pipeline.SaveToDisk(msg.ID, msg.Topic, msg.Payload)
	if err != nil {
		fmt.Printf("[ROUTER] [ERRO] Falha crítica ao salvar no pipeline: %v\n", err)
	}
}

func (d *Dispatcher) executeCircuitBreaker(msg Message) {
	d.resilience.CircuitBreakerAlert(msg.ID)
	d.metrics.RecordDelivery(0, false)
}

func (d *Dispatcher) executeThrottling(msg Message) {
	// REAL: Chama a modulação Leaky Bucket real
	d.traffic.ExecuteThrottling(msg.ID, msg.Topic, msg.Payload)
}

func (d *Dispatcher) executeActiveReplication(msg Message) {
	d.resilience.ReplicateActive(msg.ID, msg.Payload)
}

func (d *Dispatcher) executeRingBufferEviction(msg Message) {
	// REAL: Chama a estratégia de despejo rápido por estouro de volume
	d.traffic.ExecuteRingEviction(msg.ID, msg.Topic, msg.Payload)
}
