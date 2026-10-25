# Multi-platform image (Deplexo / Belmo-Coolify / any Docker host).
#
# On Deplexo the runtime root filesystem is READ-ONLY, so everything writable
# must live on the /data volume or in /tmp. On Coolify/Belmo this is also the
# RECOMMENDED build pack ("Dockerfile") because it skips Nixpacks' 600+ MB Nix
# bootstrap and its node_modules/.cache BuildKit mount (which breaks npm ci).
FROM node:22-alpine

WORKDIR /app

# Install dependencies at BUILD time only. `npm run start` must never need to
# install anything here: npm would try to rewrite /app/package-lock.json and
# fail with EROFS. npm ci (not npm install) keeps the lockfile authoritative.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund && npm cache clean --force

# Application code, seed database, static assets and the dependency guard.
# scripts/ is included so `prestart` is a harmless no-op even if a stale
# dashboard "npm start" override is used instead of the image CMD.
COPY server.js ./
COPY db.json ./
COPY public ./public
COPY scripts ./scripts

# Deplexo mounts a persistent named volume at /data. Creating the directory in
# the image with the runtime user's ownership lets Docker initialize a fresh
# volume with permissions the unprivileged process can write to.
RUN mkdir -p /data && chown node:node /data

ENV NODE_ENV=production \
    PORT=3000 \
    DB_PATH=/data/db.json

USER node

EXPOSE 3000

# Lightweight busybox wget (already in alpine). No npm/node subprocess and no
# filesystem writes, so this works with a read-only root and a 128 MB plan.
# ${PORT} is expanded by /bin/sh at runtime, so a host that injects PORT
# (Abasthan, Belmo, Coolify) is health-checked on the right port.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD wget -q -O /dev/null "http://127.0.0.1:${PORT:-3000}/health" || exit 1

# server.js drains WebSocket/SSE connections and flushes the DB on SIGTERM.
STOPSIGNAL SIGTERM

CMD ["node", "server.js"]
