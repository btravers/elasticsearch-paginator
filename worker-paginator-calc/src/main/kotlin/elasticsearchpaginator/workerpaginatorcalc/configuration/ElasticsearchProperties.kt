package elasticsearchpaginator.workerpaginatorcalc.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import java.time.Duration

@ConfigurationProperties("app.elasticsearch")
class ElasticsearchProperties(
        val scrollKeepAliveDuration: Duration,
        val pagesIndex: String,
        val pagesIndexNumberShards: Int,
        val pagesIndexNumberReplicas: Int,
        val pagesBulkSize: Int
)
