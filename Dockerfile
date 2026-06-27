FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --omit=dev

COPY waterford-fuel-decoder.js waterford-db.js waterford-ws-client.js waterford-server.js supabase-client.js ./

EXPOSE 4000

CMD ["node", "waterford-server.js"]
