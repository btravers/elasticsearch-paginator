package elasticsearchpaginator.workerpaginator.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.ConstructorBinding

@ConstructorBinding
@ConfigurationProperties("app.elasticsearch")
data class ElasticsearchProperties(
        val queryEntriesIndex: String,
        val queryEntriesIndexNumberShards: Int,
        val queryEntriesIndexNumberReplicas: Int
)
