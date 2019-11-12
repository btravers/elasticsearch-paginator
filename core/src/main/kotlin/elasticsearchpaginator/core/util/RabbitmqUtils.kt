package elasticsearchpaginator.core.util

import org.springframework.amqp.core.*

object RabbitmqUtils {

    fun AmqpAdmin.createExchange(exchangeName: String): TopicExchange {
        return ExchangeBuilder.topicExchange(exchangeName)
                .durable(true)
                .build<TopicExchange>()
                .apply {
                    this@createExchange.declareExchange(this)
                }
    }

    fun AmqpAdmin.createQueues(queueName: String, deadLetterQueueName: String, key: String, exchange: Exchange) {
        val deadLetterQueue = QueueBuilder
                .durable(deadLetterQueueName)
                .build()
        this.declareQueue(deadLetterQueue)

        val queue = QueueBuilder
                .durable(queueName)
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", deadLetterQueueName)
                .build()
        this.declareQueue(queue)

        val binding = BindingBuilder
                .bind(queue)
                .to(exchange)
                .with(key)
                .noargs()
        this.declareBinding(binding)
    }

}
