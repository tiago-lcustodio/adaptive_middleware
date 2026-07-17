# adaptive_middleware
adaptive_middleware


docker-compose -f deployments/docker-compose.yml down  
docker-compose -f deployments/docker-compose.yml up -d  
go run cmd/middleware/main.go

0A14


Falha 2  
docker stop mosquitto_unioeste_downstream  
docker start mosquitto_unioeste_downstream

Outra opção  
sudo iptables -A OUTPUT -p tcp --dport 1884 -j REJECT  
sudo iptables -D OUTPUT -p tcp --dport 1884 -j REJECT  

Se precisar derrubar o Prometheus  
sudo killall main  
# Ou force pelo número da porta:  
sudo fuser -k 8082/tcp  
