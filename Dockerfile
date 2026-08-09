FROM node:24-alpine@sha256:d32cdf619f63fe0471182d08996dd516c6275bb5fd31ae06e55a570bd9e1ad43

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund

COPY dist ./dist

ENV NODE_ENV=production
ENV GAIUS_BRIDGE_HOST=0.0.0.0
ENV GAIUS_BRIDGE_PORT=8080

EXPOSE 8080 8083

CMD ["node", "dist/main.js"]
