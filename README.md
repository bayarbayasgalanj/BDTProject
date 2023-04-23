# BDTProject
- You first start KAFKA server

- wget https://downloads.apache.org/kafka/3.4.0/kafka_2.13-3.4.0.tgz
- tar -xzf kafka_2.13-3.4.0.tgz 

- sudo useradd kafka
- sudo mv kafka_2.13-3.4.0 /var/lib/kafka
- sudo chown -R kafka:kafka /var/lib/kafka

- sudo su kafka
- cd /usr/lib/kafka
- ./bin/kafka-server-start.sh config/server.properties

<img width="292" alt="Screen Shot 2023-04-23 at 15 03 57" src="https://user-images.githubusercontent.com/18479401/233862987-3e77b2a2-19d7-4ec4-ba81-9c2b708be735.png">

