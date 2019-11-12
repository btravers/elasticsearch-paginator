package elasticsearchpaginator.core.util

import org.elasticsearch.action.ActionListener
import org.elasticsearch.common.xcontent.DeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.sort.FieldSortBuilder
import org.elasticsearch.search.sort.SortBuilder
import reactor.core.publisher.Mono

object ElasticsearchUtils {

    const val NUMBER_SHARDS_SETTING = "number_of_shards"
    const val NUMBER_REPLICAS_SETTING = "number_of_replicas"

    fun parseQuery(query: String): QueryBuilder {
        return QueryBuilders.wrapperQuery(query)
    }

    fun parseSort(sort: String): List<SortBuilder<*>> {
        return FieldSortBuilder.fromXContent(XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, sort))
    }

    fun <T> async(f: (ActionListener<T>) -> Unit): Mono<T> {
        return Mono.create { sink ->
            f(object : ActionListener<T> {
                override fun onResponse(response: T) {
                    sink.success(response)
                }

                override fun onFailure(e: Exception) {
                    sink.error(e)
                }
            })
        }
    }

}
