import type { NextConfig } from "next";

// Validate required server-side environment variables at build time.
// This surfaces misconfigured deployments immediately rather than at runtime.
const requiredServerEnvVars = [
  "NEXT_PUBLIC_SUPABASE_URL",
  "NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY",
  "SUPABASE_SERVICE_ROLE_KEY",
] as const;

// Skip validation in CI test environments where NATS may not be fully configured
if (
  process.env.NODE_ENV === "production" ||
  process.env.VALIDATE_ENV === "true"
) {
  for (const key of requiredServerEnvVars) {
    if (!process.env[key]) {
      throw new Error(
        `[next.config] Missing required environment variable: ${key}. ` +
          `Ensure it is set in your .env.local file or Vercel project settings.`,
      );
    }
  }
}

const nextConfig: NextConfig = {
  reactStrictMode: true,
  reactCompiler: true,
  compress: true,
  poweredByHeader: false,

  compiler: {
    removeConsole:
      process.env.NODE_ENV === "production"
        ? { exclude: ["error", "warn"] }
        : false,
  },

  // Security headers are also defined in vercel.json for Vercel deployments.
  // This ensures they apply in self-hosted Node environments as well.
  async headers() {
    return [
      {
        source: "/(.*)",
        headers: [
          { key: "X-Content-Type-Options", value: "nosniff" },
          { key: "X-Frame-Options", value: "DENY" },
          { key: "X-XSS-Protection", value: "1; mode=block" },
          { key: "Referrer-Policy", value: "strict-origin-when-cross-origin" },
          {
            key: "Permissions-Policy",
            value: "camera=(), microphone=(), geolocation=()",
          },
        ],
      },
    ];
  },
};

export default nextConfig;
