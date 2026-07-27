FROM node:24-alpine

WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev --ignore-scripts --no-package-lock

COPY dist ./dist

ENV NODE_ENV=production
ENV GAIUS_BRIDGE_HOST=0.0.0.0
ENV GAIUS_BRIDGE_PORT=8080

EXPOSE 8080

CMD ["node", "dist/main.js"]
