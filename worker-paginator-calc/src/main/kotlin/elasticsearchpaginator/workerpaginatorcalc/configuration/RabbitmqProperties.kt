package elasticsearchpaginator.workerpaginatorcalc.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties("app.rabbitmq")
data class RabbitmqProperties(
        val exchangeName: String,
        val computePagesKey: String,
        val deletePagesKey: String,
        val deleteQueriesKey: String
)
