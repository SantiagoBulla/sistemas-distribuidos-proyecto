FROM node:22-alpine
WORKDIR /pruebas
COPY package.json package-lock.json ./
RUN npm ci
COPY *.mjs ./
CMD ["npm", "test"]
