# pedidos360-worker

Microservicio **consumidor** de Pedidos360 (Express + amqplib). Declara la topología de
RabbitMQ y procesa los eventos de `pedidos.events` con ACK manual, reintentos con
backoff (1 s, 2 s, 4 s) y DLQ.

| Consumidor | Cola | DLQ propia | Eventos |
|---|---|---|---|
| `notificacionConsumer` | `pedidos.notificaciones.queue` | `pedidos.notificaciones.dlq` | `pedido.creado`, `pedido.estado.*`, `usuario.creado` |
| `ticketConsumer` | `pedidos.tickets.queue` | `pedidos.tickets.dlq` | `pedido.creado` → tabla `tickets` |
| `auditoriaConsumer` | `pedidos.auditoria.queue` | `pedidos.auditoria.dlq` | `#` (todos) → tabla `eventos` |
| `dlqConsumer` | las 3 DLQ | — | Mensajes muertos → log + tabla `mensajes_dlq` |

Exchanges: `pedidos.events` (**topic**), `pedidos.dlx` (**direct**, una routing key por DLQ),
`pedidos.retry` (**direct**) y `pedidos.retry.return` (headers). Colas de trabajo y DLQ:
tipo **quorum** (replicadas en el clúster).

El broker es un **clúster de 2 nodos con docker-compose**: ver [`docker/rabbitmq-cluster`](docker/rabbitmq-cluster/README.md).

- Configuración: `src/config/messaging.js`
- Topología: `src/messaging/topology.js`
- Política de ACK, reintentos y DLQ: `src/messaging/listener.js`

```bash
cp .env.example .env
pnpm install
pnpm dev        # GET http://localhost:4100/health
```

Arquitectura completa y despliegue: ver el README del proyecto (EP3).

## Kafka (EP5): streaming y analítica

Además de RabbitMQ, el worker consume de **Kafka**. Los dos brokers conviven con roles
distintos y no se pisan: RabbitMQ sigue haciendo el trabajo transaccional (notificaciones,
tickets, reintentos, DLQ) y Kafka toma el stream analítico, donde hacen falta retención,
relectura por offset y varios consumer groups leyendo lo mismo.

| Consumer group | Tópico | Qué hace |
|---|---|---|
| `pedidos360-analitica` | `pedidos.eventos.v1` | Acumula ventas por producto en Supabase y **publica** en el tópico de métricas |
| `pedidos360-proyeccion` | `pedidos.metricas.v1` | Reconstruye en memoria el ranking de más vendidos (`GET /metricas`) |
| `pedidos360-dlt-monitor` | `pedidos.eventos.dlt.v1` | Registra los mensajes muertos en la tabla `mensajes_dlq` |

El back-principal consume `pedidos.eventos.v1` con el grupo `pedidos360-back-cache`: el
**mismo tópico, otro grupo**. Cada grupo recibe todos los eventos y lleva su propio offset,
que es justo lo que RabbitMQ no permite. Dentro de un grupo, en cambio, las 3 particiones se
reparten entre sus miembros: levantar un segundo worker reparte la carga.

Política de errores del `kafkaListener` (la misma idea que `listener.js` de RabbitMQ):

| Resultado | Qué pasa |
|---|---|
| Éxito | commit del offset |
| Error recuperable, quedan intentos | espera 1 s, 2 s, 4 s y reintenta el mismo mensaje |
| Error recuperable, sin intentos | al Dead Letter Topic + commit |
| Error no recuperable (JSON roto o payload que no pasa zod) | al DLT directo, sin reintentos |

Si no se logra escribir en el DLT **no se hace commit**: el mensaje se vuelve a entregar en
vez de perderse. Los servicios de dominio son idempotentes, así que procesar dos veces es
seguro.

- Cliente Kafka: `src/config/kafkaClient.js`
- Tópicos, grupos y políticas: `src/config/kafka.js`
- Política de reintentos y DLT: `src/messaging/kafkaListener.js`
- Contrato de eventos (DTOs con zod): `src/events/eventSchemas.js` — **copia idéntica** del
  mismo archivo en el back-principal; si cambias uno, cambia el otro
- Clúster: [`docker/kafka-cluster`](docker/kafka-cluster/README.md)
- Administración de tópicos y consumer groups: `pedidos360-kafka-admin`

```bash
# El cluster Kafka debe estar levantado y la topología creada
# (pnpm topics:bootstrap en pedidos360-kafka-admin)
pnpm dev        # GET http://localhost:4100/metricas
```

Kafka es **opcional**: con `KAFKA_ENABLED=false` el worker se comporta exactamente como antes
de la EP5 y solo consume de RabbitMQ. Así se despliega en Render, que no tiene IP de salida
fija y no puede atravesar el Security Group del clúster.
