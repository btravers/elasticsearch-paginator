package elasticsearchpaginator.workerpaginatorcalc.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding
import java.time.Duration

@ConstructorBinding
@ConfigurationProperties("app.elasticsearch")
data class ElasticsearchProperties(
        val scrollKeepAliveDuration: Duration,
        val pagesIndex: String,
        val pagesIndexNumberShards: Int,
        val pagesIndexNumberReplicas: Int,
        val pagesBulkSize: Int
)
