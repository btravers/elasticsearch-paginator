package elasticsearchpaginator.workerpaginator

import elasticsearchpaginator.core.model.DeletePages
import elasticsearchpaginator.core.model.Query
import elasticsearchpaginator.workerpaginator.model.QueryEntry
import elasticsearchpaginator.workerpaginator.service.CleaningQueriesService
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.SortBuilders
import org.junit.Assert
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import reactor.test.StepVerifier
import java.time.Duration
import java.time.Instant

class ClearOutdatedQueriesIntegrationTest : AbstractIntegrationTest() {

    @Autowired
    private lateinit var cleaningQueriesService: CleaningQueriesService

    @Value("\${app.query-entries-ttl}")
    private lateinit var queryEntriesTtl: Duration

    @AfterEach
    internal fun cleanUp() {
        StepVerifier.create(this.clearQueryEntries())
                .verifyComplete()
    }

    @Test
    fun `should retrieve outdated queries and ask for associated pages deletion`() {
        val queryEntry = QueryEntry(
                query = Query(
                        index = "index1",
                        query = QueryBuilders.matchAllQuery().toString(),
                        sort = listOf(
                                SortBuilders.fieldSort("field1")
                                        .toString()
                        )
                                .joinToString(",", "[", "]"),
                        firstPageSize = 2,
                        size = 4
                ),
                lastComputationDate = Instant.now(),
                lastUseDate = Instant.now().minus(this.queryEntriesTtl)
        )
        val anOtherQueryEntry = QueryEntry(
                query = Query(
                        index = "index2",
                        query = QueryBuilders.matchAllQuery().toString(),
                        sort = listOf(
                                SortBuilders.fieldSort("field2").toString()
                        )
                                .joinToString(",", "[", "]"),
                        firstPageSize = 2,
                        size = 4
                ),
                lastComputationDate = Instant.now(),
                lastUseDate = Instant.now()
        )

        StepVerifier.create(this.saveQueryEntries(listOf(queryEntry, anOtherQueryEntry)))
                .verifyComplete()

        val rabbitMessage = this.consumeRabbitmqMessages(this.rabbitmqProperties.deletePagesKey, DeletePages::class.java)

        StepVerifier.create(this.cleaningQueriesService.getOutdatedQueriesThenDeleteRelatedPages())
                .verifyComplete()

        StepVerifier.create(this.refreshQueryEntries().thenMany(this.findAllQueryEntries()))
                .expectNextCount(2)
                .verifyComplete()

        StepVerifier.create(rabbitMessage)
                .assertNext { deleteQuery ->
                    Assert.assertEquals(
                            DeletePages(
                                    queryId = queryEntry.query.hash()
                            ),
                            deleteQuery
                    )
                }
                .verifyComplete()
    }

}
