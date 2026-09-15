# adaptive_middleware
adaptive_middleware

## para derrubar os containeres, subir e executar
docker-compose -f deployments/docker-compose.yml down  
docker-compose -f deployments/docker-compose.yml up -d  
go run cmd/middleware/main.go

## template de mensagem enviada pelo TTS
0A14

## Como simular a Falha Total (Falha 2)  
docker stop mosquitto_unioeste_downstream  
docker start mosquitto_unioeste_downstream  
(não tem funcionado adequadamente)

Outra opção que funciona adequadamente:  
sudo iptables -A OUTPUT -p tcp --dport 1884 -j REJECT  
sudo iptables -D OUTPUT -p tcp --dport 1884 -j REJECT  

## Se precisar derrubar o Prometheus quando ele trava:   
sudo killall main (as vezes só isso resolve)  
Ou force pelo número da porta:  
sudo fuser -k 8082/tcp (funciona eventualmente)

Se falhar derrrubar os containeres todos da aplicação:  
sudo systemctl stop docker containerd  
sudo killall -9 containerd-shim containerd-shim-runc-v2  
sudo systemctl start containerd docker  
sudo docker rm -f $(sudo docker ps -aq)  
sudo docker compose -f deployments/docker-compose.yml up -d  

## a aplicação receiver é:  
https://github.com/tiago-lcustodio/mqtt_backend_receiver
