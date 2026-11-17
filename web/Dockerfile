# Etapa 1: compila el tablero React
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY index.html vite.config.js ./
COPY src ./src
RUN npm run build

# Etapa 2: Nginx sirve el tablero y actúa como proxy inverso hacia las réplicas del gateway
FROM nginx:1.27-alpine
RUN apk add --no-cache openssl && mkdir -p /etc/nginx/tls && \
    openssl req -x509 -nodes -newkey rsa:2048 -days 365 \
      -keyout /etc/nginx/tls/movia.key -out /etc/nginx/tls/movia.crt \
      -subj "/C=CO/L=Bogota/O=MOVIA/CN=localhost"
COPY nginx/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80 443
