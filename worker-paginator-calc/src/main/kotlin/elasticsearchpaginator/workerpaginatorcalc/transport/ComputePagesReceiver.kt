package elasticsearchpaginator.workerpaginatorcalc.transport

import com.fasterxml.jackson.databind.ObjectMapper
import elasticsearchpaginator.core.model.Query
import elasticsearchpaginator.core.transport.AbstractRabbitmqReceiver
import elasticsearchpaginator.workerpaginatorcalc.configuration.RabbitmqConfiguration
import elasticsearchpaginator.workerpaginatorcalc.service.ComputePagesService
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.rabbitmq.Receiver

@Component
class ComputePagesReceiver(override val receiver: Receiver,
                           override val mapper: ObjectMapper,
                           override val applicationContext: ApplicationContext,
                           private val computePagesService: ComputePagesService,
                           private val rabbitmqConfiguration: RabbitmqConfiguration) : AbstractRabbitmqReceiver<Query>() {

    override val queueName = this.rabbitmqConfiguration.computePagesQueueName()
    override val eventClass = Query::class.java

    override fun eventHandler(event: Query): Mono<Void> {
        return this.computePagesService.computePages(event)
    }

}
