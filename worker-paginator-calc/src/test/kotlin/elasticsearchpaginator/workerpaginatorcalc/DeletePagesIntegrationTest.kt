package elasticsearchpaginator.workerpaginatorcalc

import elasticsearchpaginator.core.model.DeleteQuery
import elasticsearchpaginator.core.util.RabbitmqUtils.createExchange
import elasticsearchpaginator.core.util.RabbitmqUtils.createQueues
import elasticsearchpaginator.workerpaginatorcalc.configuration.RabbitmqProperties
import elasticsearchpaginator.workerpaginatorcalc.model.Page
import elasticsearchpaginator.workerpaginatorcalc.repository.PageRepository
import elasticsearchpaginator.workerpaginatorcalc.service.DeletePagesService
import org.jeasy.random.EasyRandom
import org.jeasy.random.EasyRandomParameters
import org.jeasy.random.FieldPredicates
import org.jeasy.random.api.Randomizer
import org.junit.Assert
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.amqp.core.AmqpAdmin
import org.springframework.beans.factory.annotation.Autowired
import reactor.core.publisher.Flux
import reactor.rabbitmq.Receiver
import reactor.test.StepVerifier
import java.util.stream.Stream
import kotlin.random.Random

class DeletePagesIntegrationTest : AbstractIntegrationTest() {

    @Autowired
    private lateinit var deletePagesService: DeletePagesService

    @Autowired
    private lateinit var pageRepository: PageRepository

    @Autowired
    private lateinit var receiver: Receiver

    @Autowired
    private lateinit var amqpAdmin: AmqpAdmin

    @Autowired
    private lateinit var rabbitmqProperties: RabbitmqProperties

    @AfterEach
    internal fun cleanUp() {
        StepVerifier.create(this.clearPages())
                .verifyComplete()
    }

    @Test
    fun `should delete all pages matching the given query id`() {
        val queryId = "queryId"
        val queryIdFieldPredicate = FieldPredicates
                .named("queryId")
                .and(
                        FieldPredicates
                                .ofType(String::class.java)
                )
                .and(
                        FieldPredicates
                                .inClass(Page::class.java)
                )
        val pageFieldPredicate = FieldPredicates
                .named("page")
                .and(
                        FieldPredicates
                                .ofType(Long::class.java)
                )
                .and(
                        FieldPredicates
                                .inClass(Page::class.java)
                )
        val searchAfterQueryParametersFieldPredicate = FieldPredicates
                .named("searchAfterQueryParameters")
                .and(
                        FieldPredicates
                                .ofType(Any::class.java)
                )
                .and(
                        FieldPredicates
                                .inClass(Page::class.java)
                )
        val parametersForPage = EasyRandomParameters()
                .randomize(pageFieldPredicate, Randomizer { Random.nextLong(0, 100) })
                .randomize(searchAfterQueryParametersFieldPredicate, Randomizer { emptyMap<String, Any>() })
        val parametersForPageWithGivenQueryId = EasyRandomParameters()
                .randomize(queryIdFieldPredicate, Randomizer { queryId })
                .randomize(pageFieldPredicate, Randomizer { Random.nextLong(0, 100) })
                .randomize(searchAfterQueryParametersFieldPredicate, Randomizer { emptyMap<String, Any>() })

        val pages = Stream.concat(
                EasyRandom(parametersForPageWithGivenQueryId).objects(Page::class.java, 10),
                EasyRandom(parametersForPage).objects(Page::class.java, 10)
        )

        StepVerifier.create(this.pageRepository.save(Flux.fromStream(pages)).then(this.refreshPages()))
                .verifyComplete()

        val rabbitMessage = this.consumeRabbitmqMessages()

        StepVerifier.create(this.deletePagesService.deletePages(queryId))
                .verifyComplete()

        StepVerifier.create(this.refreshPages().thenMany(this.findAllPages()))
                .recordWith { mutableListOf() }
                .thenConsumeWhile { true }
                .expectRecordedMatches { savedPages ->
                    savedPages.size == 10 && savedPages.all { page -> page.queryId != queryId }
                }
                .verifyComplete()

        StepVerifier.create(rabbitMessage)
                .assertNext { deleteQuery ->
                    Assert.assertEquals(
                            DeleteQuery(
                                    queryId = queryId
                            ),
                            deleteQuery
                    )
                }
                .verifyComplete()
    }

    private fun consumeRabbitmqMessages(): Flux<DeleteQuery> {
        val queueName = "${this.rabbitmqProperties.exchangeName}.${this.rabbitmqProperties.deleteQueriesKey}"
        val deadLetterQueueName = "${this.rabbitmqProperties.exchangeName}.${this.rabbitmqProperties.deleteQueriesKey}.dead-letter"

        val exchange = this.amqpAdmin.createExchange(this.rabbitmqProperties.exchangeName)
        this.amqpAdmin.createQueues(queueName, deadLetterQueueName, this.rabbitmqProperties.deleteQueriesKey, exchange)

        return this.receiver.consumeAutoAck(queueName)
                .map { delivery -> this.mapper.readValue(delivery.body, DeleteQuery::class.java) }
                .take(1)
    }

}
