package elasticsearchpaginator.workerpaginatorcalc

import com.fasterxml.jackson.databind.ObjectMapper
import elasticsearchpaginator.core.model.Query
import elasticsearchpaginator.core.model.SortBuilderList
import elasticsearchpaginator.core.util.ElasticsearchUtils
import elasticsearchpaginator.workerpaginatorcalc.configuration.ElasticsearchProperties
import elasticsearchpaginator.workerpaginatorcalc.service.ComputePagesService
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
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortBuilders
import org.elasticsearch.search.sort.SortOrder
import org.jeasy.random.EasyRandom
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Instant
import java.util.stream.Collectors

class WorkerPaginatorCalcIntegrationTest : AbstractIntegrationTest() {

    private val indexName = "articles"

    @Autowired
    private lateinit var restHighLevelClient: RestHighLevelClient

    @Autowired
    private lateinit var mapper: ObjectMapper

    @Autowired
    private lateinit var computePagesService: ComputePagesService

    @Autowired
    private lateinit var elasticsearchProperties: ElasticsearchProperties

    @Test
    fun `should compute pages`() {
        val articles = EasyRandom().objects(Article::class.java, 20).collect(Collectors.toList())

        StepVerifier.create(this.saveArticles(articles))
                .verifyComplete()

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

        StepVerifier.create(this.computePagesService.computePages(query))
                .verifyComplete()

        StepVerifier.create(this.refreshPages().thenMany(this.findAllPages()))
                .expectNextCount(5) // TODO check pages content
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

    private fun refreshPages(): Mono<Void> {
        return Mono.just(
                RefreshRequest()
                        .indices(this.elasticsearchProperties.pagesIndex)
        )
                .flatMap { refreshRequest ->
                    ElasticsearchUtils.async<RefreshResponse> { actionListener ->
                        this.restHighLevelClient.indices().refreshAsync(refreshRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .then()
    }

    private fun findAllPages(): Flux<SearchHit> {
        return Mono.just(
                SearchSourceBuilder()
                        .query(
                                QueryBuilders.matchAllQuery()
                        )
                        .size(10000)
        )
                .map { searchSourceBuilder ->
                    SearchRequest()
                            .indices(this.elasticsearchProperties.pagesIndex)
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
