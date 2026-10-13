import { registerNotificacionConsumer } from "./notificacionConsumer.js";
import { registerTicketConsumer } from "./ticketConsumer.js";
import { registerDlqConsumer } from "./dlqConsumer.js";

// Registra todos los consumidores (uno por dominio funcional).
export async function registerConsumers(channel) {
  await registerNotificacionConsumer(channel);
  await registerTicketConsumer(channel);
  await registerDlqConsumer(channel);
}
