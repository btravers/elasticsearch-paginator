package elasticsearchpaginator.workerpaginatorcalc.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import elasticsearchpaginator.core.util.ElasticsearchUtils.async
import elasticsearchpaginator.workerpaginatorcalc.configuration.ElasticsearchProperties
import elasticsearchpaginator.workerpaginatorcalc.exception.UnexpectedQueryException
import org.elasticsearch.action.search.*
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.DeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentFactory
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.SearchModule
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.springframework.stereotype.Repository
import reactor.core.publisher.Mono
import java.util.*


@Repository
class EntityElasticsearchRepository(private val restHighLevelClient: RestHighLevelClient,
                                    private val mapper: ObjectMapper,
                                    private val elasticsearchProperties: ElasticsearchProperties) {

    private val scrollKeepAlive = TimeValue.timeValueMillis(this.elasticsearchProperties.scrollKeepAliveDuration.toMillis())

    fun searchScroll(index: String, query: String, sort: String, size: Int): Mono<SearchResponse> {
        val includes = this.mapper.readValue<List<Any>>(sort)
                .map { s ->
                    when (s) {
                        is String -> s
                        is Map<*, *> -> s.keys.first() as String
                        else -> throw UnexpectedQueryException("Could not read sort value '$s'")
                    }
                }
                .map { fieldName -> fieldName.removeSuffix(".keyword") }
                .map { fieldName -> """"$fieldName"""" }
                .joinToString(",", "[", "]")

        val payload = """
            {
                "_source": $includes,
                "query": $query,
                "sort": $sort
            }
        """.trimIndent()

        return Mono.fromCallable {
            val searchSourceBuilder = SearchSourceBuilder()
            val searchModule = SearchModule(Settings.EMPTY, false, Collections.emptyList())
            val parser = XContentFactory.xContent(XContentType.JSON)
                    .createParser(
                            NamedXContentRegistry(searchModule.namedXContents),
                            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            payload
                    )
            searchSourceBuilder.parseXContent(parser)
            searchSourceBuilder
        }
                .map { searchSourceBuilder ->
                    SearchRequest()
                            .indices(index)
                            .source(searchSourceBuilder)
                            .scroll(this.scrollKeepAlive)
                }
                .flatMap { searchRequest ->
                    async<SearchResponse> { actionListener ->
                        this.restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT, actionListener)
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
                        this.restHighLevelClient.scrollAsync(searchScrollRequest, RequestOptions.DEFAULT, actionListener)
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
                        this.restHighLevelClient.clearScrollAsync(clearScrollRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
    }
}
