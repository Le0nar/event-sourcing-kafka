## For run

1. run docker containers : docker compose -f docker-compose.yml up -d
2. run all services via "go run main.go" from service directories

### Services:
1) write-service - producer service
2) read-service - first consumer service
3) notification-service - second consumer service

### Components
![plot](/components.png)

### Http methods exmaples

1) localhost:8080/payment [post]

    body: 
{
    "id": "550e8400-e29b-41d4-a716-446655442212",
    "userId": "330e8400-e29b-41d4-a716-446655440000",
    "status": "complete"
}

2) localhost:8081/payment/330e8400-e29b-41d4-a716-446655440000 [get]