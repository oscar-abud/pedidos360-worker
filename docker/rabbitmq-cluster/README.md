# Clúster RabbitMQ de 2 nodos (docker-compose en AWS EC2)

Broker de Pedidos360: dos nodos (`rabbit1`, `rabbit2`) unidos en un clúster. Las colas
de trabajo y las DLQ son **quorum**, así que quedan replicadas en ambos nodos.

| Puerto | Uso |
|---|---|
| 5672 | AMQP del nodo 1 |
| 5673 | AMQP del nodo 2 |
| 15672 | Management UI + API HTTP (ve todo el clúster) |

## Levantarlo en la EC2

```bash
git clone https://github.com/oscar-abud/pedidos360-worker.git
cd pedidos360-worker/docker/rabbitmq-cluster
cp .env.example .env
nano .env          # usuario, contraseña larga y cookie secreto
docker compose up -d
docker compose ps  # ambos nodos "healthy" (tarda ~20 s)
docker exec pedidos360-rabbit1 rabbitmqctl cluster_status   # Running Nodes: rabbit@rabbit1, rabbit@rabbit2
```

Management UI: `http://<IP_EC2>:15672` → **Overview → Nodes** muestra los 2 nodos.

## Conexión de los servicios

`RABBITMQ_URL` acepta los dos nodos separados por coma. Si uno no responde, el servicio
se conecta al otro:

```
RABBITMQ_URL=amqp://<usuario>:<clave>@<IP_EC2>:5672,amqp://<usuario>:<clave>@<IP_EC2>:5673
```

## Qué pasa si se cae un nodo

Las colas quorum necesitan mayoría (2 de 2). Con un nodo caído quedan en estado
`minority`: RabbitMQ **pausa** las publicaciones nuevas en vez de arriesgar perderlas, y al
volver el nodo se procesan sin pérdida (probado). Para seguir operando con un nodo caído
se necesitan 3 nodos; la EC2 de la capa gratuita (1 GB) no da para un tercero.
