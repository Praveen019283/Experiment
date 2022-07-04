package producer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.databind.util.StdDateFormat
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.github.javafaker.Faker
import data.Person
import org.apache.kafka.clients.producer.ProducerRecord

fun produceData(){
    val producer = createProducer();
    val faker = Faker();
    var i = 1;
    for(i in 1..100) {

        val fakePerson = Person(
            firstName = faker.name().firstName(),
            lastName = faker.name().lastName(),
            birthDate = faker.date().birthday()
        )
        val jsonMapper = ObjectMapper().apply {
            registerKotlinModule()
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            setDateFormat(StdDateFormat())
        }
        val fakePersonJson = jsonMapper.writeValueAsString(fakePerson)

        try {
            for(j in 1..3){
                val sentData = producer.send(ProducerRecord("test", fakePersonJson));
                sentData.get()
                println("Inserted ${i}")
                if(sentData.isDone) break;
            }
        }catch (e: Exception){
            println("Sent Data to dead letter queue");
        }


    }
}

fun main() {
    produceData();
}