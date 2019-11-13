package elasticsearchpaginator.workerpaginatorcalc

import elasticsearchpaginator.core.model.Query
import elasticsearchpaginator.core.model.SortBuilderList
import elasticsearchpaginator.core.util.ElasticsearchUtils
import elasticsearchpaginator.workerpaginatorcalc.model.Page
import elasticsearchpaginator.workerpaginatorcalc.service.ComputePagesService
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.SortBuilders
import org.elasticsearch.search.sort.SortOrder
import org.jeasy.random.EasyRandom
import org.junit.Assert
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Instant
import java.util.stream.Collectors

class ComputePagesIntegrationTest : AbstractIntegrationTest() {

    private val indexName = "articles"

    val articles = EasyRandom().objects(Article::class.java, 22).collect(Collectors.toList())

    @Autowired
    private lateinit var computePagesService: ComputePagesService

    @BeforeAll
    internal fun setUp() {
        StepVerifier.create(this.saveArticles(articles))
                .verifyComplete()
    }

    @AfterAll
    internal fun tearDown() {
        StepVerifier.create(this.deleteArticlesIndex())
                .verifyComplete()
    }

    @AfterEach
    internal fun cleanUp() {
        StepVerifier.create(this.clearPages())
                .verifyComplete()
    }

    @Test
    fun `should compute pages for a basic query`() {
        val query = Query(
                index = indexName,
                size = 4,
                firstPageSize = 2,
                query = QueryBuilders.matchAllQuery(),
                sort = SortBuilderList(
                        listOf(
                                SortBuilders
                                        .fieldSort("id.keyword")
                                        .order(SortOrder.ASC)
                        )
                )
        )
        val sortedArticles = articles
                .sortedBy { article -> article.id }

        StepVerifier.create(this.computePagesService.computePages(query))
                .verifyComplete()

        StepVerifier.create(this.refreshPages().thenMany(this.findAllPages()))
                .assertNext { searchHit ->
                    Assert.assertEquals(
                            Page(
                                    queryId = query.hash(),
                                    page = 1,
                                    searchAfterQueryParameters = mapOf(
                                            "id" to sortedArticles[1].id
                                    )
                            ),
                            searchHit
                    )
                }
                .assertNext { searchHit ->
                    Assert.assertEquals(
                            Page(
                                    queryId = query.hash(),
                                    page = 2,
                                    searchAfterQueryParameters = mapOf(
                                            "id" to sortedArticles[5].id
                                    )
                            ),
                            searchHit
                    )
                }
                .assertNext { searchHit ->
                    Assert.assertEquals(
                            Page(
                                    queryId = query.hash(),
                                    page = 3,
                                    searchAfterQueryParameters = mapOf(
                                            "id" to sortedArticles[9].id
                                    )
                            ),
                            searchHit
                    )
                }
                .assertNext { searchHit ->
                    Assert.assertEquals(
                            Page(
                                    queryId = query.hash(),
                                    page = 4,
                                    searchAfterQueryParameters = mapOf(
                                            "id" to sortedArticles[13].id
                                    )
                            ),
                            searchHit
                    )
                }
                .assertNext { searchHit ->
                    Assert.assertEquals(
                            Page(
                                    queryId = query.hash(),
                                    page = 5,
                                    searchAfterQueryParameters = mapOf(
                                            "id" to sortedArticles[17].id
                                    )
                            ),
                            searchHit
                    )
                }
                .verifyComplete()
    }

    @Test
    fun `should compute pages with sort on nested field`() {
        val query = Query(
                index = indexName,
                size = 4,
                firstPageSize = 2,
                query = QueryBuilders.matchAllQuery(),
                sort = SortBuilderList(
                        listOf(
                                SortBuilders
                                        .fieldSort("content.title.keyword")
                                        .order(SortOrder.ASC)
                        )
                )
        )
        val sortedArticles = articles
                .sortedBy { article -> article.content.title }

        StepVerifier.create(this.computePagesService.computePages(query))
                .verifyComplete()

        StepVerifier.create(this.refreshPages().thenMany(this.findAllPages()))
                .assertNext { searchHit ->
                    Assert.assertEquals(
                            Page(
                                    queryId = query.hash(),
                                    page = 1,
                                    searchAfterQueryParameters = mapOf(
                                            "content" to mapOf(
                                                    "title" to sortedArticles[1].content.title
                                            )
                                    )
                            ),
                            searchHit
                    )

                }
                .assertNext { searchHit ->
                    Assert.assertEquals(
                            Page(
                                    queryId = query.hash(),
                                    page = 2,
                                    searchAfterQueryParameters = mapOf(
                                            "content" to mapOf(
                                                    "title" to sortedArticles[5].content.title
                                            )
                                    )
                            ),
                            searchHit
                    )
                }
                .assertNext { searchHit ->
                    Assert.assertEquals(
                            Page(
                                    queryId = query.hash(),
                                    page = 3,
                                    searchAfterQueryParameters = mapOf(
                                            "content" to mapOf(
                                                    "title" to sortedArticles[9].content.title
                                            )
                                    )
                            ),
                            searchHit
                    )
                }
                .assertNext { searchHit ->
                    Assert.assertEquals(
                            Page(
                                    queryId = query.hash(),
                                    page = 4,
                                    searchAfterQueryParameters = mapOf(
                                            "content" to mapOf(
                                                    "title" to sortedArticles[13].content.title
                                            )
                                    )
                            ),
                            searchHit
                    )
                }
                .assertNext { searchHit ->
                    Assert.assertEquals(
                            Page(
                                    queryId = query.hash(),
                                    page = 5,
                                    searchAfterQueryParameters = mapOf(
                                            "content" to mapOf(
                                                    "title" to sortedArticles[17].content.title
                                            )
                                    )
                            ),
                            searchHit
                    )
                }
                .verifyComplete()
    }

    private fun saveArticles(articles: List<Article>): Mono<Void> {
        return Flux.fromIterable(articles)
                .map { article ->
                    IndexRequest()
                            .index(indexName)
                            .id(article.id)
                            .source(this.mapper.writeValueAsBytes(article), XContentType.JSON)
                }
                .collectList()
                .map { indexRequests  ->
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

    private fun deleteArticlesIndex(): Mono<Void> {
        return Mono.just(
                DeleteIndexRequest()
                        .indices(indexName)
        )
                .flatMap { deleteIndexRequest ->
                    ElasticsearchUtils.async<AcknowledgedResponse> { actionListener ->
                        this.restHighLevelClient.indices().deleteAsync(deleteIndexRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .then()
    }

    data class Article(
            val id: String,
            val content: Content,
            val creationDate: Instant,
            val author: Author
    ) {

        data class Content(
                val title: String,
                val head: String,
                val body: String
        )

        data class Author(
                val id: String,
                val firstName: String,
                val lastName: String
        )

    }

}
