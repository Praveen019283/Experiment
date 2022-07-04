package consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.util.Properties;

fun createConsumer(): Consumer<String, String> {
    val props = Properties()
    props["bootstrap.servers"] = "localhost:9092"
    props["group.id"] = "hello-consumer"
    props["key.deserializer"] = StringDeserializer::class.java
    props["value.deserializer"] = StringDeserializer::class.java
    return KafkaConsumer(props)
}

