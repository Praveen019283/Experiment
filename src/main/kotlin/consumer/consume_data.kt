package consumer

import java.time.Duration

fun consumeData(){
    val consumer = createConsumer()
    consumer.subscribe(listOf("test"))

    while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        println("Consumed ${records.count()} records")

        records.iterator().forEach {
            val message = it.value()
            println("Message: $message")
        }
    }
}

fun main() {
    consumeData();
}