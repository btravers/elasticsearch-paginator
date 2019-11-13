package elasticsearchpaginator.workerpaginator

import elasticsearchpaginator.core.model.Query
import elasticsearchpaginator.core.util.ElasticsearchUtils
import elasticsearchpaginator.workerpaginator.model.QueryEntry
import elasticsearchpaginator.workerpaginator.service.CleaningQueriesService
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.SortBuilders
import org.junit.Assert
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Instant

class DeleteQueryIntegrationTest : AbstractIntegrationTest() {

    @Autowired
    private lateinit var cleaningQueriesService: CleaningQueriesService

    @Test
    fun `should delete the query entry`() {
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
                lastUseDate = Instant.now()
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

        StepVerifier.create(this.cleaningQueriesService.deleteQuery(queryEntry.query.hash()))
                .verifyComplete()

        StepVerifier.create(this.refreshQueryEntries().thenMany(this.findAllQueryEntries()))
                .assertNext { savedQueryEntry ->
                    Assert.assertEquals(anOtherQueryEntry, savedQueryEntry)
                }
                .verifyComplete()
    }

    private fun saveQueryEntries(queryEntries: List<QueryEntry>): Mono<Void> {
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

}
