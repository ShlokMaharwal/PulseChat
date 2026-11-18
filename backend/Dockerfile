FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
COPY packages/shared/package.json packages/shared/package.json
COPY packages/gateway/package.json packages/gateway/package.json
COPY packages/processor/package.json packages/processor/package.json
RUN --mount=type=cache,id=pulsechat-npm,target=/root/.npm npm ci --no-audit --no-fund
COPY tsconfig*.json ./
COPY packages ./packages
RUN npm run build

FROM build AS integration
COPY tests ./tests
COPY vitest.integration.config.ts tsconfig.tests.json ./
CMD ["npm", "run", "test:integration"]

FROM build AS production-deps
RUN npm prune --omit=dev --no-audit --no-fund

FROM node:22-alpine AS runtime
ARG SERVICE=gateway
RUN test "$SERVICE" = gateway || test "$SERVICE" = processor
ENV NODE_ENV=production SERVICE=$SERVICE
WORKDIR /app
COPY --from=production-deps --chown=node:node /app/package*.json ./
COPY --from=production-deps --chown=node:node /app/node_modules ./node_modules
COPY --from=production-deps --chown=node:node /app/packages ./packages
COPY --chown=node:node tests/smoke.mjs ./tests/smoke.mjs
USER node
EXPOSE 8080
CMD ["sh", "-c", "exec node packages/$SERVICE/dist/index.js"]
