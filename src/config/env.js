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
};
