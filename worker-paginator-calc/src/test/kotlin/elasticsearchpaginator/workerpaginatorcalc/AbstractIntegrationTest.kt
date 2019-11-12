package elasticsearchpaginator.workerpaginatorcalc

import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.util.TestPropertyValues
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit.jupiter.SpringExtension
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.elasticsearch.ElasticsearchContainer

@ExtendWith(SpringExtension::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = [AbstractIntegrationTest.Initializer::class])
@AutoConfigureWebTestClient
abstract class AbstractIntegrationTest {

    companion object {
        private val RABBITMQ_USERNAME = "guest"
        private val RABBITMQ_PASSWORD = "guest"

        val elasticsearchContainer = ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch-oss:7.4.2")
        val rabbitmqContainer = RabbitMQContainer("rabbitmq:3.8")
                .withUser(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    }

    internal class Initializer : ApplicationContextInitializer<ConfigurableApplicationContext> {
        override fun initialize(configurableApplicationContext: ConfigurableApplicationContext) {
            rabbitmqContainer.start()
            elasticsearchContainer.start()

            TestPropertyValues.of(
                    "spring.elasticsearch.rest.uris=${elasticsearchContainer.httpHostAddress}",
                    "spring.rabbitmq.host=${rabbitmqContainer.containerIpAddress}",
                    "spring.rabbitmq.port=${rabbitmqContainer.firstMappedPort}",
                    "spring.rabbitmq.username=$RABBITMQ_USERNAME",
                    "spring.rabbitmq.password=$RABBITMQ_PASSWORD"
            )
                    .applyTo(configurableApplicationContext.environment)
        }
    }

}
