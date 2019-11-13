package elasticsearchpaginator.workerpaginator

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import elasticsearchpaginator.core.util.ElasticsearchUtils
import elasticsearchpaginator.core.util.RabbitmqUtils.createExchange
import elasticsearchpaginator.core.util.RabbitmqUtils.createQueues
import elasticsearchpaginator.workerpaginator.configuration.ElasticsearchProperties
import elasticsearchpaginator.workerpaginator.configuration.RabbitmqProperties
import elasticsearchpaginator.workerpaginator.model.QueryEntry
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.reindex.BulkByScrollResponse
import org.elasticsearch.index.reindex.DeleteByQueryRequest
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.amqp.core.AmqpAdmin
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.util.TestPropertyValues
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit.jupiter.SpringExtension
import org.testcontainers.containers.RabbitMQContainer
import org.testcontainers.elasticsearch.ElasticsearchContainer
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.rabbitmq.Receiver
import reactor.test.StepVerifier
import java.time.Duration

@ExtendWith(SpringExtension::class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = [AbstractIntegrationTest.Initializer::class])
@AutoConfigureWebTestClient
abstract class AbstractIntegrationTest {

    init {
        StepVerifier.setDefaultTimeout(Duration.ofSeconds(5))
    }

    @Autowired
    protected lateinit var restHighLevelClient: RestHighLevelClient

    @Autowired
    protected lateinit var mapper: ObjectMapper

    @Autowired
    protected lateinit var receiver: Receiver

    @Autowired
    protected lateinit var amqpAdmin: AmqpAdmin

    @Autowired
    protected lateinit var rabbitmqProperties: RabbitmqProperties

    @Autowired
    protected lateinit var elasticsearchProperties: ElasticsearchProperties

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

    protected fun refreshQueryEntries(): Mono<Void> {
        return Mono.just(
                RefreshRequest()
                        .indices(this.elasticsearchProperties.queryEntriesIndex)
        )
                .flatMap { refreshRequest ->
                    ElasticsearchUtils.async<RefreshResponse> { actionListener ->
                        this.restHighLevelClient.indices().refreshAsync(refreshRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .then()
    }

    protected fun findAllQueryEntries(): Flux<QueryEntry> {
        return Mono.just(
                SearchSourceBuilder()
                        .query(
                                QueryBuilders.matchAllQuery()
                        )
                        .size(10000)
        )
                .map { searchSourceBuilder ->
                    SearchRequest()
                            .indices(this.elasticsearchProperties.queryEntriesIndex)
                            .source(searchSourceBuilder)
                }
                .flatMap { searchRequest ->
                    ElasticsearchUtils.async<SearchResponse> { actionListener ->
                        this.restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .flatMapIterable { searchResponse ->
                    searchResponse.hits
                }
                .map { searchHit ->
                    this.mapper.readValue<QueryEntry>(searchHit.sourceRef.streamInput())
                }
    }

    protected fun saveQueryEntries(queryEntries: List<QueryEntry>): Mono<Void> {
        return Flux.fromIterable(queryEntries)
                .map { queryEntry ->
                    IndexRequest()
                            .index(this.elasticsearchProperties.queryEntriesIndex)
                            .id(queryEntry.query.hash())
                            .source(this.mapper.writeValueAsBytes(queryEntry), XContentType.JSON)
                }
                .collectList()
                .map { indexRequests ->
                    BulkRequest()
                            .add(indexRequests)
                            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                }
                .flatMap { bulkRequest ->
                    ElasticsearchUtils.async<BulkResponse> { actionListener ->
                        this.restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .then()
    }

    protected fun clearQueryEntries(): Mono<Void> {
        return Mono.just(
                DeleteByQueryRequest(this.elasticsearchProperties.queryEntriesIndex)
                        .setQuery(
                                QueryBuilders.matchAllQuery()
                        )
                        .setRefresh(true)
        )
                .flatMap { deleteByQueryRequest ->
                    ElasticsearchUtils.async<BulkByScrollResponse> { actionListener ->
                        this.restHighLevelClient.deleteByQueryAsync(deleteByQueryRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .then()
    }

    protected fun <T> consumeRabbitmqMessages(key: String, clazz: Class<T>): Flux<T> {
        val queueName = "${this.rabbitmqProperties.exchangeName}.$key"
        val deadLetterQueueName = "${this.rabbitmqProperties.exchangeName}.$key.dead-letter"

        val exchange = this.amqpAdmin.createExchange(this.rabbitmqProperties.exchangeName)
        this.amqpAdmin.createQueues(queueName, deadLetterQueueName, key, exchange)

        return this.receiver.consumeAutoAck(queueName)
                .map { delivery -> this.mapper.readValue(delivery.body, clazz) }
                .take(1)
                .timeout(Duration.ofSeconds(1))
    }

}
