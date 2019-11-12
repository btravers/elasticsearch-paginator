package elasticsearchpaginator.workerpaginator.configuration

import org.springframework.boot.context.properties.ConfigurationProperties

@ConfigurationProperties("app.elasticsearch")
data class ElasticsearchProperties(
        val queryEntriesIndex: String,
        val queryEntriesIndexNumberShards: Int,
        val queryEntriesIndexNumberReplicas: Int
)
