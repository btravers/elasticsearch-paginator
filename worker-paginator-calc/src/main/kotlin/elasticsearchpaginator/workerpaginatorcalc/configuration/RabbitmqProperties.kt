package elasticsearchpaginator.workerpaginatorcalc.configuration

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("app.rabbitmq")
data class RabbitmqProperties(
        val exchangeName: String,
        val computePagesKey: String,
        val deletePagesKey: String,
        val deleteQueriesKey: String
)
