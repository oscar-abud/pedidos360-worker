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
