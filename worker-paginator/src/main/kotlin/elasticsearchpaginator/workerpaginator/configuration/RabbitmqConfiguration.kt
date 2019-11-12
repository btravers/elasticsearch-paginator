package elasticsearchpaginator.workerpaginator.configuration

import elasticsearchpaginator.core.util.RabbitmqUtils.createExchange
import elasticsearchpaginator.core.util.RabbitmqUtils.createQueues
import org.springframework.amqp.core.AmqpAdmin
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(RabbitmqProperties::class)
class RabbitmqConfiguration(private val amqpAdmin: AmqpAdmin,
                            private val rabbitmqProperties: RabbitmqProperties,
                            @Value("\${spring.application.name}") private val applicationName: String) : InitializingBean {

    override fun afterPropertiesSet() {
        val exchange = this.amqpAdmin.createExchange(this.rabbitmqProperties.exchangeName)
        this.amqpAdmin.createQueues(this.queryQueueName(), this.queryDeadLetterQueueName(), this.rabbitmqProperties.queriesKey, exchange)
        this.amqpAdmin.createQueues(this.deleteQueryQueueName(), this.deleteDeadLetterQueryQueueName(), this.rabbitmqProperties.deleteQueriesKey, exchange)
    }

    fun queryQueueName(): String {
        return "${this.applicationName}.${this.rabbitmqProperties.exchangeName}.${this.rabbitmqProperties.queriesKey}"
    }

    fun queryDeadLetterQueueName(): String {
        return "${this.applicationName}.${this.rabbitmqProperties.exchangeName}.${this.rabbitmqProperties.queriesKey}.dead-letter"
    }

    fun deleteQueryQueueName(): String {
        return "${this.applicationName}.${this.rabbitmqProperties.exchangeName}.${this.rabbitmqProperties.deleteQueriesKey}"
    }

    fun deleteDeadLetterQueryQueueName(): String {
        return "${this.applicationName}.${this.rabbitmqProperties.exchangeName}.${this.rabbitmqProperties.deleteQueriesKey}.dead-letter"
    }
}
