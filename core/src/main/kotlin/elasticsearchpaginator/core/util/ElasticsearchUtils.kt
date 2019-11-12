package elasticsearchpaginator.core.util

import org.elasticsearch.action.ActionListener
import reactor.core.publisher.Mono

object ElasticsearchUtils {

    const val NUMBER_SHARDS_SETTING = "number_of_shards"
    const val NUMBER_REPLICAS_SETTING = "number_of_replicas"

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
