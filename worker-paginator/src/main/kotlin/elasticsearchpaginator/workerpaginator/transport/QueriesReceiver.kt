package elasticsearchpaginator.workerpaginator.transport

import com.fasterxml.jackson.databind.ObjectMapper
import elasticsearchpaginator.core.model.Query
import elasticsearchpaginator.core.transport.AbstractRabbitmqReceiver
import elasticsearchpaginator.workerpaginator.configuration.RabbitmqConfiguration
import elasticsearchpaginator.workerpaginator.service.QueriesService
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.rabbitmq.Receiver

@Component
class QueriesReceiver(override val receiver: Receiver,
                      override val mapper: ObjectMapper,
                      override val applicationContext: ApplicationContext,
                      private val queriesService: QueriesService,
                      private val rabbitmqConfiguration: RabbitmqConfiguration) : AbstractRabbitmqReceiver<Query>() {

    override val queueName = this.rabbitmqConfiguration.queryQueueName()
    override val eventClass = Query::class.java

    override fun eventHandler(event: Query): Mono<Void> {
        return this.queriesService.upsertQueryAndAskForPagesComputation(event)
    }

}
