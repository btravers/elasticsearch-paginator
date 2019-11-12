package elasticsearchpaginator.workerpaginatorcalc.transport

import com.fasterxml.jackson.databind.ObjectMapper
import elasticsearchpaginator.core.model.DeletePages
import elasticsearchpaginator.core.transport.AbstractRabbitmqReceiver
import elasticsearchpaginator.workerpaginatorcalc.configuration.RabbitmqConfiguration
import elasticsearchpaginator.workerpaginatorcalc.service.DeletePagesService
import org.springframework.context.ApplicationContext
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.rabbitmq.Receiver

@Component
class DeletePagesReceiver(override val receiver: Receiver,
                          override val mapper: ObjectMapper,
                          override val applicationContext: ApplicationContext,
                          private val deletePagesService: DeletePagesService,
                          private val rabbitmqConfiguration: RabbitmqConfiguration) : AbstractRabbitmqReceiver<DeletePages>() {

    override val queueName = this.rabbitmqConfiguration.deletePagesQueueName()
    override val eventClass = DeletePages::class.java

    override fun eventHandler(event: DeletePages): Mono<Void> {
        return this.deletePagesService.deletePages(event.queryId)
    }

}
