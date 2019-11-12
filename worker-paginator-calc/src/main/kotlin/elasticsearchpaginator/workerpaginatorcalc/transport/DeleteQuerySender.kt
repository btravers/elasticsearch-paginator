package elasticsearchpaginator.workerpaginatorcalc.transport

import com.fasterxml.jackson.databind.ObjectMapper
import elasticsearchpaginator.core.model.DeleteQuery
import elasticsearchpaginator.workerpaginatorcalc.configuration.RabbitmqProperties
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.rabbitmq.OutboundMessage
import reactor.rabbitmq.Sender

@Component
class DeleteQuerySender(private val sender: Sender,
                        private val mapper: ObjectMapper,
                        private val rabbitmqProperties: RabbitmqProperties) {

    fun sendDeleteQueryEvent(deleteQuery: DeleteQuery): Mono<Void> {
        return this.sender.send(
                Mono.just(
                        OutboundMessage(
                                this.rabbitmqProperties.exchangeName,
                                this.rabbitmqProperties.deleteQueriesKey,
                                this.mapper.writeValueAsBytes(deleteQuery)
                        )
                )
        )
    }

}
