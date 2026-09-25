// SLUICE_API_PROXY (e.g. http://localhost:8080) serves the Sluice API under
// /sluice-api on the dashboard's own origin, so only the dashboard's port has to
// be reachable, as in GitHub Codespaces. Pair it with NEXT_PUBLIC_API_URL=/sluice-api.
const proxy = process.env.SLUICE_API_PROXY;

/** @type {import('next').NextConfig} */
const nextConfig = {
  async rewrites() {
    return proxy ? [{ source: "/sluice-api/:path*", destination: `${proxy}/:path*` }] : [];
  },
};

export default nextConfig;
