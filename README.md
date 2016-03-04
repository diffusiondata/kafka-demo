# Kafka/Diffusion Demo

First download and install kafka as per:

http://kafka.apache.org/documentation.html#quickstart

Then start zookeeper and kafka

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

Set DIFFUSION_HOME to your diffusion install and then
Build this project:

mvn clean install

Copy the created dar to diffusion. 

cp target/kafkaprod-1.0-SNAPSHOT.dar $DIFFUSION_HOME/deploy/

There are some issues with the diffusion demos and this demo, so make sure the regular
demos are not deployed.

start Diffusion

cd $DIFFUSION_HOME/bin
sh diffusion.sh

If all is good Diffusion should start and the demo should connect to the kafka broker.

Send some messages to kafka using the console producer:

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test

You should see these messages being picked up by diffusion and then being published
out to the console under the kafka topic.
