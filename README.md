# 启动测试环境
```
docker run -d --name zookeeper -p 2181:2181  -t wurstmeister/zookeeper

docker run -p 9200:9200 -p 9300:9300 --name elasticsearch -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.12.0

docker run -d --name kafka-to-elasticsearch --link kafka1 --link elasticsearch -v /opt/docker/config:/config-dir logstash:7.12.1  logstash -f /config-dir/kafka-to-elasticsearch.conf

docker run -d --name kafka1 -p 9092:9092 -e KAFKA_BROKER_ID=0 -e KAFKA_ZOOKEEPER_CONNECT=192.168.15.147:2181/kafka -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://192.168.15.147:9092 -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 --env KAFKA_HEAP_OPTS="-Xmx256M -Xms128M" -v /etc/localtime:/etc/localtime wurstmeister/kafka
```
# 创建kafka topic
```
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server 192.168.15.147:9092  --replication-factor 1 --partitions 1 --topic ckb_inputs
docker exec -it kafka1 /opt/kafka/bin/kafka-topics.sh --create --bootstrap-server 192.168.15.147:9092  --replication-factor 1 --partitions 1 --topic ckb_outputs
```

logstash 配置
```
input {
  kafka {
    bootstrap_servers => "192.168.15.147:9092"
    group_id => "logstash_group"
    decorate_events => true
    client_id => "logstash_client_1"
    auto_offset_reset => "latest"
    topics => ["ckb_outputs","ckb_inputs"]
    codec => json {
                     charset => "UTF-8"
                 }
  }
}


output {
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
}
```
创建ES索引
```
 
curl  -X DELETE http://192.168.15.147:9200/job
curl  -X DELETE http://192.168.15.147:9200/ckb_inputs
curl  -X DELETE http://192.168.15.147:9200/ckb_outputs

curl -H "Content-Type:application/json" -X PUT -d ' {"settings":{"index":{"refresh_interval":"10s","number_of_shards":"1","translog":{"flush_threshold_size":"256mb","sync_interval":"10s"},"number_of_replicas":"0","merge":{"policy":{"floor_segment":"50mb","max_merged_segment":"5gb"}}}},"mappings":{"properties":{"num":{"type":"long"},"name":{"type":"keyword"}}}} ' http://192.168.15.147:9200/job
curl -H "Content-Type:application/json" -X PUT -d ' {"settings":{"index":{"refresh_interval":"10s","number_of_shards":"1","translog":{"flush_threshold_size":"256mb","sync_interval":"10s"},"number_of_replicas":"0","merge":{"policy":{"floor_segment":"50mb","max_merged_segment":"5gb"}}}},"mappings":{"properties":{"@timestamp":{"type":"date"},"num":{"type":"long"},"@version":{"type":"text","fields":{"keyword":{"ignore_above":256,"type":"keyword"}}},"lock":{"properties":{"args":{"type":"binary"},"codeHash":{"type":"binary"},"hashType":{"type":"text","fields":{"keyword":{"ignore_above":256,"type":"keyword"}}}}},"type":{"properties":{"args":{"type":"binary"},"codeHash":{"type":"binary"},"hashType":{"type":"text","fields":{"keyword":{"ignore_above":256,"type":"keyword"}}}}},"capacity":{"type":"long"}}}}' http://192.168.15.147:9200/ckb_outputs
curl -H "Content-Type:application/json" -X PUT -d ' {"settings":{"index":{"refresh_interval":"10s","number_of_shards":"1","translog":{"flush_threshold_size":"256mb","sync_interval":"10s"},"number_of_replicas":"0","merge":{"policy":{"floor_segment":"50mb","max_merged_segment":"5gb"}}}},"mappings":{"properties":{"@timestamp":{"type":"date"},"num":{"type":"long"},"@version":{"type":"text","fields":{"keyword":{"ignore_above":256,"type":"keyword"}}},"index":{"type":"long"},"txHash":{"type":"binary"},"since":{"type":"long"}}}}' http://192.168.15.147:9200/ckb_inputs

```

