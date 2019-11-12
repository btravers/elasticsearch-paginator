package elasticsearchpaginator.workerpaginatorcalc.configuration

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
        this.amqpAdmin.createQueues(this.computePagesQueueName(), this.computePagesDeadLetterQueueName(), this.rabbitmqProperties.computePagesKey, exchange)
        this.amqpAdmin.createQueues(this.deletePagesQueueName(), this.deletePagesDeadLetterQueueName(), this.rabbitmqProperties.deletePagesKey, exchange)
    }

    fun computePagesQueueName(): String {
        return "${this.applicationName}.${this.rabbitmqProperties.exchangeName}.${this.rabbitmqProperties.computePagesKey}"
    }

    fun computePagesDeadLetterQueueName(): String {
        return "${this.applicationName}${this.rabbitmqProperties.exchangeName}.${this.rabbitmqProperties.computePagesKey}.dead-letter"
    }

    fun deletePagesQueueName(): String {
        return "${this.applicationName}.${this.rabbitmqProperties.exchangeName}.${this.rabbitmqProperties.deletePagesKey}"
    }

    fun deletePagesDeadLetterQueueName(): String {
        return "${this.applicationName}.${this.rabbitmqProperties.exchangeName}.${this.rabbitmqProperties.deletePagesKey}.dead-letter"
    }

}
