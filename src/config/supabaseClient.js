import { createClient } from "@supabase/supabase-js";
import { env } from "./env.js";

// Cliente único de Supabase (Singleton), igual que en el back. Secret key:
// bypassa RLS, solo los servicios backend la tienen.
export const supabase = createClient(env.supabaseUrl, env.supabaseSecretKey, {
  auth: { persistSession: false },
});
