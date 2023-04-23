# ckb
docker run -d --name zookeeper -p 2181:2181  -t wurstmeister/zookeeper

docker run -p 9200:9200 -p 9300:9300 --name elasticsearch -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.12.0

docker run -d --name kafka-to-elasticsearch --link kafka1 --link elasticsearch -v /opt/docker/config:/config-dir logstash:7.12.1  logstash -f /config-dir/kafka-to-elasticsearch.conf

docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server 192.168.15.147:9092  --replication-factor 1 --partitions 1 --topic ckb_inputs
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server 192.168.15.147:9092  --replication-factor 1 --partitions 1 --topic ckb_outputs


docker run -d --name kafka1 -p 9092:9092 -e KAFKA_BROKER_ID=0 -e KAFKA_ZOOKEEPER_CONNECT=192.168.15.147:2181/kafka -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://192.168.15.147:9092 -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 --env KAFKA_HEAP_OPTS="-Xmx256M -Xms128M" -v /etc/localtime:/etc/localtime wurstmeister/kafka


if [@metadata][kafka][topic] == "ckb_outputs" {
		elasticsearch { 
			index => "ckb_outputs"
			hosts => ["http://192.168.15.147:9200"]
		}
	}
	if [@metadata][kafka][topic] == "ckb_inputs" {
		elasticsearch { 
			index => "ckb_inputs"
			hosts => ["http://192.168.15.147:9200"]
		}
	}

  curl -X PUT "192.168.15.147:9200/ckb_outputs?pretty"
  curl -X PUT "192.168.15.147:9200/ckb_inputs?pretty"


