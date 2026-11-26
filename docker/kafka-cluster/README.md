# Clúster Kafka — Pedidos360

Arquitectura **Zookeeper + 3 brokers + Kafka-UI** con docker-compose, levantada en una
**EC2 propia**, separada de la del clúster RabbitMQ (que sigue intacto en su instancia).

Kafka no reemplaza a RabbitMQ: lo complementa. RabbitMQ conserva el trabajo transaccional
(notificaciones, tickets, reintentos, DLQ) y Kafka toma el **stream analítico**, donde hacen
falta retención, relectura por offset y varios consumer groups leyendo lo mismo.

| Servicio | Contenedor | Puerto host | Uso |
|---|---|---|---|
| zookeeper | `pedidos360-zookeeper` | 2181 | Coordinación del clúster (**no abrir a internet**) |
| kafka1 | `pedidos360-kafka1` | 9092 | Broker 1 (listener EXTERNAL) |
| kafka2 | `pedidos360-kafka2` | 9094 | Broker 2 |
| kafka3 | `pedidos360-kafka3` | 9096 | Broker 3 |
| kafka-ui | `pedidos360-kafka-ui` | 8080 | Administración de brokers y tópicos |

Ninguno choca con los puertos ya usados por el proyecto: 4000 (back), 4100 (worker),
4200 (rabbit-admin), 4300 (kafka-admin), 5173 (front), 5672/5673/15672 (RabbitMQ).

## Levantarlo en la EC2

Requiere una instancia con **4 GB de RAM (t3.medium)**: 3 brokers con heap de 512 MB más
Zookeeper y la UI suman ~3 GB. Con 2 GB hay que bajar `KAFKA_HEAP_OPTS` a `-Xmx256m`.

```bash
git clone https://github.com/oscar-abud/pedidos360-worker.git
cd pedidos360-worker/docker/kafka-cluster
cp .env.example .env
nano .env                  # KAFKA_EXTERNAL_HOST = IP pública de ESTA EC2
docker compose up -d
docker compose ps          # los 5 contenedores deben quedar "healthy"

# Verificar que los 3 brokers se ven entre sí
docker exec pedidos360-kafka1 kafka-broker-api-versions \
  --bootstrap-server kafka1:29092,kafka2:29092,kafka3:29092 | grep -c "id:"
```

Luego, desde `pedidos360-kafka-admin`, crear la topología de tópicos:

```bash
pnpm topics:bootstrap
```

## Security Group de AWS

El clúster usa **PLAINTEXT** (sin autenticación en los brokers), así que la protección es
el Security Group. Reglas de entrada:

| Puerto | Origen |
|---|---|
| 9092, 9094, 9096 | **Solo tu IP** (`<tu-ip>/32`) |
| 8080 | **Solo tu IP** |
| 2181 | Nadie (uso interno de Docker) |
| 22 | Solo tu IP |

Consecuencia: los servicios desplegados en **Render no pueden conectarse** a Kafka, porque
Render no entrega una IP de salida fija en el plan gratuito. Por eso el back y el worker
traen el flag `KAFKA_ENABLED`: en Render va en `false` (siguen funcionando solo con
RabbitMQ, igual que hoy) y en local va en `true` apuntando a esta EC2. Para habilitar Kafka
también en Render habría que añadir SASL/PLAIN a los listeners y abrir el puerto.

## Conexión de los servicios

```bash
KAFKA_BROKERS=<IP-EC2>:9092,<IP-EC2>:9094,<IP-EC2>:9096
KAFKA_ENABLED=true
```

Se pasan los tres brokers para que el cliente tenga de dónde pedir metadata aunque uno esté
caído. No hace falta que sean todos: basta con que uno responda.

## Qué pasa si se cae un broker

Los tópicos se crean con **replication factor 3** y **min.insync.replicas 2**, y los
productores escriben con `acks=all`. Con un broker caído quedan 2 réplicas en sincronía, que
es justo el mínimo: el sistema **sigue produciendo y consumiendo sin perder mensajes**, y
Kafka elige un nuevo líder para las particiones que lideraba el nodo caído.

```bash
docker compose stop kafka2    # el flujo sigue funcionando
docker compose start kafka2   # el ISR vuelve a 3 en segundos
```

Si cayeran dos brokers a la vez, las escrituras se rechazarían (no habría forma de cumplir
`min.insync.replicas=2`) en lugar de aceptarlas y perderlas en silencio.
