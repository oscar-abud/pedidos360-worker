# pedidos360-worker

Microservicio **consumidor** de Pedidos360 (Express + amqplib). Declara la topología de
RabbitMQ y procesa los eventos de `pedidos.events` con ACK manual, reintentos con
backoff (1 s, 2 s, 4 s) y DLQ.

| Consumidor | Cola | Eventos |
|---|---|---|
| `notificacionConsumer` | `pedidos.notificaciones.queue` | `pedido.creado`, `pedido.estado.*`, `usuario.creado` |
| `ticketConsumer` | `pedidos.tickets.queue` | `pedido.creado` → tabla `tickets` |
| `dlqConsumer` | `pedidos.dlq` | Mensajes muertos → log + tabla `mensajes_dlq` |

- Configuración: `src/config/messaging.js`
- Topología: `src/messaging/topology.js`
- Política de ACK, reintentos y DLQ: `src/messaging/listener.js`

```bash
cp .env.example .env
pnpm install
pnpm dev        # GET http://localhost:4100/health
```

Arquitectura completa y despliegue: ver el README del proyecto (EP3).
