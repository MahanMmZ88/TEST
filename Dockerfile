# Deplexo-compatible image: the runtime root filesystem is READ-ONLY.
# Everything writable must live on the /data volume or in /tmp.
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
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:3000/health || exit 1

CMD ["node", "server.js"]
