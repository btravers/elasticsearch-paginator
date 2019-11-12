package elasticsearchpaginator.core.configuration

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.boot.autoconfigure.amqp.RabbitProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.publisher.Mono
import reactor.rabbitmq.*

@Configuration
@EnableAutoConfiguration
class RabbitmqAutoConfiguration(private val rabbitProperties: RabbitProperties) : DisposableBean {

    override fun destroy() {
        this.connectionMono()
                .doOnNext(Connection::close)
                .block()
    }

    @Bean
    fun connectionMono(): Mono<Connection> {
        return Mono.fromCallable {
            ConnectionFactory()
                    .apply {
                        this.host = rabbitProperties.host
                        this.port = rabbitProperties.port
                        this.username = rabbitProperties.username
                        this.password = rabbitProperties.password
                        this.useNio()
                    }
                    .newConnection()
        }
                .cache()
    }

    @Bean
    fun receiver(connectionMono: Mono<Connection>): Receiver {
        return RabbitFlux.createReceiver(
                ReceiverOptions()
                        .connectionMono(connectionMono)
        )
    }

    @Bean
    fun sender(connectionMono: Mono<Connection>): Sender {
        return RabbitFlux.createSender(
                SenderOptions()
                        .connectionMono(connectionMono)
        )
    }

}
