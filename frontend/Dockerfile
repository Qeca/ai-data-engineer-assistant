FROM node:22-alpine

WORKDIR /app

ARG NEXT_PUBLIC_API_URL=/api/backend
ARG BACKEND_INTERNAL_URL=http://backend:8000
ENV NEXT_PUBLIC_API_URL=$NEXT_PUBLIC_API_URL
ENV BACKEND_INTERNAL_URL=$BACKEND_INTERNAL_URL

COPY frontend/package*.json ./
RUN npm install

COPY frontend ./
COPY shared ../shared
RUN npm run build

EXPOSE 3000
CMD ["npm", "run", "start"]
