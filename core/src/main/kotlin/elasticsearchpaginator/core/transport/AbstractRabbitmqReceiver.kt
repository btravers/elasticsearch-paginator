package elasticsearchpaginator.core.transport

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.InitializingBean
import org.springframework.boot.ExitCodeGenerator
import org.springframework.boot.SpringApplication
import org.springframework.context.ApplicationContext
import reactor.core.publisher.Mono
import reactor.rabbitmq.Receiver

abstract class AbstractRabbitmqReceiver<T> : InitializingBean {

    private val logger = LoggerFactory.getLogger(AbstractRabbitmqReceiver::class.java)

    abstract val queueName: String
    abstract val receiver: Receiver
    abstract val mapper: ObjectMapper
    abstract val applicationContext: ApplicationContext
    abstract val eventClass: Class<T>

    abstract fun eventHandler(event: T): Mono<Void>

    override fun afterPropertiesSet() {
        this.receiver
                .consumeManualAck(queueName)
                .flatMap { acknowledgableDelivery ->
                    Mono.just(acknowledgableDelivery.body)
                            .map { body -> this.mapper.readValue(body, eventClass) }
                            .flatMap(this::eventHandler)
                            .doOnSuccess { acknowledgableDelivery.ack() }
                            .onErrorResume { err ->
                                logger.error("Unexpected error during pagination computation", err)
                                acknowledgableDelivery.nack(false)
                                Mono.empty()
                            }
                }
                .doOnError { err -> logger.error("Stop listening queue {} due to error", queueName, err) }
                .doOnComplete { logger.error("Unexpected end of listening queue {}", queueName) }
                .subscribe(
                        {},
                        { SpringApplication.exit(applicationContext, ExitCodeGenerator { 1 }) },
                        { SpringApplication.exit(applicationContext, ExitCodeGenerator { 0 }) }
                )
    }

}
