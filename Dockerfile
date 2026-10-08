FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev
COPY . .
# each service overrides CMD in docker-compose.yml
CMD ["node", "services/anomaly/index.js"]
