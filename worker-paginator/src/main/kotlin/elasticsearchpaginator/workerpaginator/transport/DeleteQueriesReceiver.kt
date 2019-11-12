package elasticsearchpaginator.workerpaginator.transport

import com.fasterxml.jackson.databind.ObjectMapper
import elasticsearchpaginator.core.model.DeleteQuery
import elasticsearchpaginator.core.transport.AbstractRabbitmqReceiver
import elasticsearchpaginator.workerpaginator.configuration.RabbitmqConfiguration
import elasticsearchpaginator.workerpaginator.service.CleaningQueriesService
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.rabbitmq.Receiver

@Component
class DeleteQueriesReceiver(override val receiver: Receiver,
                            override val mapper: ObjectMapper,
                            override val applicationContext: ApplicationContext,
                            private val cleaningQueriesService: CleaningQueriesService,
                            private val rabbitmqConfiguration: RabbitmqConfiguration) : AbstractRabbitmqReceiver<DeleteQuery>() {

    override val queueName = this.rabbitmqConfiguration.deleteQueryQueueName()
    override val eventClass = DeleteQuery::class.java

    override fun eventHandler(event: DeleteQuery): Mono<Void> {
        return this.cleaningQueriesService.deleteQuery(event.queryId)
    }

}
