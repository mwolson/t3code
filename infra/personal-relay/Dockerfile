FROM node:26.12.1-alpine
WORKDIR /app
COPY dist/ /app/
COPY migrations/ /app/migrations/personal/
COPY relay-migrations/ /app/migrations/relay/
USER node
ENV NODE_ENV=production
EXPOSE 8080
ENTRYPOINT ["node", "/app/main.mjs"]
