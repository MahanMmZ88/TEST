# Official lightweight Node.js LTS image
FROM node:20-alpine

# Set working directory inside container
WORKDIR /app

# Copy dependency manifests
COPY package*.json ./

# Install production dependencies
RUN npm ci --omit=dev || npm install --omit=dev

# Copy application files
COPY . .

# Ensure node user owns the app directory for database write access (db.json)
RUN chown -R node:node /app

# Switch to non-root user for security
USER node

# Expose default application port (DockHosting routes to $PORT)
EXPOSE 3000

# Set production environment variables
ENV NODE_ENV=production \
    PORT=3000

# Start command
CMD ["node", "server.js"]
