package elasticsearchpaginator.workerpaginator.transport

import com.fasterxml.jackson.databind.ObjectMapper
import elasticsearchpaginator.core.model.Query
import elasticsearchpaginator.workerpaginator.configuration.RabbitmqProperties
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.rabbitmq.OutboundMessage
import reactor.rabbitmq.Sender

@Component
class ComputePagesSender(private val sender: Sender,
                         private val mapper: ObjectMapper,
                         private val rabbitmqProperties: RabbitmqProperties) {

    fun sendComputePagesEvent(query: Query): Mono<Void> {
        return this.sender.send(
                Mono.just(
                        OutboundMessage(
                                this.rabbitmqProperties.exchangeName,
                                this.rabbitmqProperties.computePagesKey,
                                this.mapper.writeValueAsBytes(query)
                        )
                )
        )
    }

}
