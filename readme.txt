//prepare kafka

docker-compose up -d

//kafka set partitions

kafka-topics.sh --bootstrap-server localhost:9092 --create --topic test-topic --partitions 3 --replication-factor 1

//pm2 create cluster

pm2 delete all && pm2 start consumer.js -i 5 -f