FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --no-audit --no-fund; else npm install --no-audit --no-fund; fi
COPY tsconfig.json tsconfig.app.json angular.json ./
COPY src ./src
RUN npx ng build

FROM nginx:1.31.2-alpine
COPY --from=build /app/dist/browser /usr/share/nginx/html
