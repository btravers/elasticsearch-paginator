package elasticsearchpaginator.workerpaginatorcalc.repository

import elasticsearchpaginator.core.util.ElasticsearchUtils.async
import elasticsearchpaginator.workerpaginatorcalc.configuration.ElasticsearchProperties
import org.elasticsearch.action.search.*
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.FieldSortBuilder
import org.elasticsearch.search.sort.SortBuilder
import org.springframework.stereotype.Repository
import reactor.core.publisher.Mono

@Repository
class EntityElasticsearchRepository(private val highLevelClient: RestHighLevelClient,
                                    private val elasticsearchProperties: ElasticsearchProperties) {

    private val scrollKeepAlive = TimeValue.timeValueMillis(this.elasticsearchProperties.scrollKeepAliveDuration.toMillis())

    fun searchScroll(index: String, query: QueryBuilder, sort: List<SortBuilder<*>>, size: Int): Mono<SearchResponse> {

        val includes = sort
                .filterIsInstance<FieldSortBuilder>()
                .map { fieldSortBuilder ->
                    fieldSortBuilder.fieldName
                }
                .map { fieldName ->
                    fieldName.removeSuffix(".keyword")
                }
                .toTypedArray()

        return Mono.just(
                SearchSourceBuilder()
                        .fetchSource(includes, null)
                        .query(query)
                        .let { searchSourceBuilder ->
                            sort.fold(searchSourceBuilder) { acc, s -> acc.sort(s) }
                        }
                        .size(size)
        )
                .map { searchSourceBuilder ->
                    SearchRequest()
                            .indices(index)
                            .source(searchSourceBuilder)
                            .scroll(this.scrollKeepAlive)
                }
                .flatMap { searchRequest ->
                    async<SearchResponse> { actionListener ->
                        this.highLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
    }

    fun scroll(scrollId: String): Mono<SearchResponse> {
        return Mono.just(
                SearchScrollRequest(scrollId)
                        .scroll(this.scrollKeepAlive)
        )
                .flatMap { searchScrollRequest ->
                    async<SearchResponse> { actionListener ->
                        this.highLevelClient.scrollAsync(searchScrollRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
    }

    fun clearScroll(scrollId: String): Mono<ClearScrollResponse> {
        return Mono.just(
                ClearScrollRequest()
                        .apply { this.addScrollId(scrollId) }
        )
                .flatMap { clearScrollRequest ->
                    async<ClearScrollResponse> { actionListener ->
                        this.highLevelClient.clearScrollAsync(clearScrollRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
    }
}
