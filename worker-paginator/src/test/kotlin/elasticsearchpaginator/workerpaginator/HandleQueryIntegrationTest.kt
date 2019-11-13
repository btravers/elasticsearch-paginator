package elasticsearchpaginator.workerpaginator

import elasticsearchpaginator.core.model.Query
import elasticsearchpaginator.workerpaginator.model.QueryEntry
import elasticsearchpaginator.workerpaginator.service.QueriesService
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.SortBuilders
import org.junit.Assert
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import reactor.test.StepVerifier
import java.time.Instant
import java.util.concurrent.TimeoutException

class HandleQueryIntegrationTest : AbstractIntegrationTest() {

    @Autowired
    private lateinit var queriesService: QueriesService

    @AfterEach
    internal fun cleanUp() {
        StepVerifier.create(this.clearQueryEntries())
                .verifyComplete()
    }

    @Test
    fun `should create a new query entry and request pages computation for that query`() {
        val now = Instant.now()
        val query = Query(
                index = "index1",
                query = QueryBuilders.matchAllQuery().toString(),
                sort = listOf(
                        SortBuilders.fieldSort("field1")
                                .toString()
                )
                        .joinToString(",", "[", "]"),
                firstPageSize = 2,
                size = 4
        )

        val rabbitMessage = this.consumeRabbitmqMessages(this.rabbitmqProperties.computePagesKey, Query::class.java)

        StepVerifier.create(this.queriesService.upsertQueryAndAskForPagesComputation(query))
                .verifyComplete()

        StepVerifier.create(this.refreshQueryEntries().thenMany(this.findAllQueryEntries()))
                .assertNext { queryEntry ->
                    Assert.assertEquals(query, queryEntry.query)
                    Assert.assertTrue(queryEntry.lastUseDate > now)
                    Assert.assertTrue(queryEntry.lastComputationDate > now)
                    Assert.assertTrue(queryEntry.lastComputationDate > queryEntry.lastUseDate)
                }
                .verifyComplete()

        StepVerifier.create(rabbitMessage)
                .expectNext(query)
                .verifyComplete()
    }

    @Test
    fun `should request pages computation when last computation date is too recent`() {
        val now = Instant.now()
        val query = Query(
                index = "index1",
                query = QueryBuilders.matchAllQuery().toString(),
                sort = listOf(
                        SortBuilders.fieldSort("field1")
                                .toString()
                )
                        .joinToString(",", "[", "]"),
                firstPageSize = 2,
                size = 4
        )
        val queryEntry = QueryEntry(
                query = query,
                lastUseDate = now,
                lastComputationDate = now
        )

        StepVerifier.create(this.saveQueryEntries(listOf(queryEntry)).then(this.refreshQueryEntries()))
                .verifyComplete()

        val rabbitMessage = this.consumeRabbitmqMessages(this.rabbitmqProperties.computePagesKey, Query::class.java)

        StepVerifier.create(this.queriesService.upsertQueryAndAskForPagesComputation(query))
                .verifyComplete()

        StepVerifier.create(this.refreshQueryEntries().thenMany(this.findAllQueryEntries()))
                .assertNext { entry ->
                    Assert.assertEquals(query, entry.query)
                    Assert.assertTrue(entry.lastUseDate > now)
                    Assert.assertEquals(now, entry.lastComputationDate)
                }
                .verifyComplete()

        StepVerifier.create(rabbitMessage)
                .verifyError(TimeoutException::class.java)
    }

}
