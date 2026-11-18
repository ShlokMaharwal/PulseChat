import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    strictPort: true,
    proxy: {
      '/api': {
        target: process.env.PULSECHAT_PROXY_TARGET ?? 'http://localhost:8080',
        changeOrigin: true,
        rewrite: (path) =>
          process.env.PULSECHAT_DIRECT_GATEWAY === '1'
            ? path.replace(/^\/api/, '')
            : path,
      },
      '/ws': {
        target: process.env.PULSECHAT_PROXY_TARGET ?? 'http://localhost:8080',
        ws: true,
      },
    },
  },
  test: {
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    clearMocks: true,
  },
});
