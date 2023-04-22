# BDTProject
- You first start KAFKA server

- wget https://downloads.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz
- tar -xzf kafka_2.13-3.4.0.tgz 

- sudo useradd kafka
- sudo mv kafka_2.13-3.4.0 /usr/local/kafka
- sudo chown -R kafka:kafka /var/lib/kafka

- sudo su kafka
- cd /usr/lib/kafka
- ./bin/kafka-server-start.sh config/server.properties

<img width="301" alt="Screen Shot 2023-04-22 at 16 49 37" src="https://user-images.githubusercontent.com/18479401/233808016-e5c317ee-2cdc-4631-b7b6-8488dbe12394.png">
