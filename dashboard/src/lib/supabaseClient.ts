import { createClient } from "@supabase/supabase-js";

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
const supabasePublishableKey = process.env.NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabasePublishableKey) {
  throw new Error(
    "[supabaseClient] Missing NEXT_PUBLIC_SUPABASE_URL or NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY. " +
    "Ensure both are set in your .env.local or Vercel project settings."
  );
}

/**
 * SERVER-ONLY Supabase client that uses the service-role key.
 * This client bypasses Row-Level Security and must NEVER be imported
 * in client-side code or `use client` components.
 * Import this from API routes and server-side lib modules only.
 */
export const supabaseServer = createClient(
  supabaseUrl,
  supabaseServiceRoleKey ?? supabasePublishableKey, // Falls back to anon key in dev if service role key not configured
  {
    auth: {
      // Server processes must not persist auth tokens
      persistSession: false,
      autoRefreshToken: false,
    },
  }
);

/**
 * Public Supabase client that uses the anon/publishable key.
 * Safe for use in browser-facing code. Subject to Row-Level Security policies.
 * @deprecated Prefer supabaseServer in API routes. This client exists for
 * future client-side Realtime subscriptions initiated from the browser.
 */
export const supabase = createClient(supabaseUrl, supabasePublishableKey, {
  auth: {
    persistSession: false,
  },
});
