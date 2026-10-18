import { registerNotificacionConsumer } from "./notificacionConsumer.js";
import { registerTicketConsumer } from "./ticketConsumer.js";
import { registerAuditoriaConsumer } from "./auditoriaConsumer.js";
import { registerDlqConsumers } from "./dlqConsumer.js";

// Registra todos los consumidores (uno por dominio funcional + uno por DLQ).
export async function registerConsumers(channel) {
  await registerNotificacionConsumer(channel);
  await registerTicketConsumer(channel);
  await registerAuditoriaConsumer(channel);
  await registerDlqConsumers(channel);
}
