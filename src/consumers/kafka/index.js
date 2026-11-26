import { registerAnaliticaConsumer } from "./analiticaConsumer.js";
import { registerProyeccionConsumer } from "./proyeccionConsumer.js";
import { registerDltConsumer } from "./dltConsumer.js";

// Registra los tres consumidores de Kafka (uno por consumer group). Se hace en
// paralelo porque cada uno abre su propia conexion y su propio rebalanceo.
export async function registerKafkaConsumers() {
  await Promise.all([
    registerAnaliticaConsumer(),
    registerProyeccionConsumer(),
    registerDltConsumer(),
  ]);
}
