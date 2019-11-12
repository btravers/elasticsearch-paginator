package elasticsearchpaginator.workerpaginator.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties("app.rabbitmq")
data class RabbitmqProperties(
        val exchangeName: String,
        val queriesKey: String,
        val deleteQueriesKey: String,
        val deletePagesKey: String,
        val computePagesKey: String
)
