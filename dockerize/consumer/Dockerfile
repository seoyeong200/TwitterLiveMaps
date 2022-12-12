FROM node:10-alpine
WORKDIR /usr/src/app
USER root
COPY package*.json ./
RUN npm install
RUN npm install -g nodemon
# COPY --chown=node:node . .
COPY . .
CMD ["nodemon","-L", "src/consumer.js"]
