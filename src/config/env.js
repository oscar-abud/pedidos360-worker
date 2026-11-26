import "dotenv/config";

function required(value, key) {
  if (!value || value.trim() === "") {
    console.warn(`[config] Falta ${key} en .env`);
    return "";
  }
  return value;
}

export const env = {
  port: Number(process.env.PORT) || 4100,
  rabbitmqUrl: required(process.env.RABBITMQ_URL, "RABBITMQ_URL"),
  supabaseUrl: required(process.env.SUPABASE_URL, "SUPABASE_URL"),
  supabaseSecretKey: required(process.env.SUPABASE_SECRET_KEY, "SUPABASE_SECRET_KEY"),

  // Kafka (clúster de docker/kafka-cluster). Complementa a RabbitMQ para el
  // stream analítico, no lo reemplaza.
  //
  // Con KAFKA_ENABLED=false el worker se comporta exactamente como antes de la
  // EP5: solo consume de RabbitMQ. Se usa en Render, que no tiene IP de salida
  // fija y por eso no puede atravesar el Security Group del clúster.
  kafkaEnabled: process.env.KAFKA_ENABLED === "true",
  kafkaBrokers: (process.env.KAFKA_BROKERS ?? "")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean),
  kafkaClientId: process.env.KAFKA_CLIENT_ID || "pedidos360-worker",
};
