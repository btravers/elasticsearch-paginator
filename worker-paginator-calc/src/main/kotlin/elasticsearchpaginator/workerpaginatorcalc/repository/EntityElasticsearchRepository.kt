package elasticsearchpaginator.workerpaginatorcalc.repository

import elasticsearchpaginator.core.util.ElasticsearchUtils.async
import elasticsearchpaginator.core.util.ElasticsearchUtils.parseQuery
import elasticsearchpaginator.core.util.ElasticsearchUtils.parseSort
import elasticsearchpaginator.workerpaginatorcalc.configuration.ElasticsearchProperties
import org.elasticsearch.action.search.*
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.FieldSortBuilder
import org.springframework.stereotype.Repository
import reactor.core.publisher.Mono

@Repository
class EntityElasticsearchRepository(private val highLevelClient: RestHighLevelClient,
                                    private val elasticsearchProperties: ElasticsearchProperties) {

    private val scrollKeepAlive = TimeValue.timeValueMillis(this.elasticsearchProperties.scrollKeepAliveDuration.toMillis())

    fun searchScroll(index: String, query: String, sort: String, size: Int): Mono<SearchResponse> {
        val parsedQuery = parseQuery(query)
        val parsedSorts = parseSort(sort)

        val includes = parsedSorts
                .filterIsInstance<FieldSortBuilder>()
                .map { fieldSortBuilder ->
                    fieldSortBuilder.fieldName
                }
                .toTypedArray()

        return Mono.just(
                SearchSourceBuilder()
                        .fetchSource(includes, null)
                        .query(parsedQuery)
                        .let { searchSourceBuilder ->
                            parsedSorts.fold(searchSourceBuilder) { acc, s -> acc.sort(s) }
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
