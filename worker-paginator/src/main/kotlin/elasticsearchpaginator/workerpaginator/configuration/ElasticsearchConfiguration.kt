package elasticsearchpaginator.workerpaginator.configuration

import elasticsearchpaginator.workerpaginator.repository.QueryEntryRepository
import org.springframework.beans.factory.InitializingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(ElasticsearchProperties::class)
class ElasticsearchConfiguration(private val queryEntryRepository: QueryEntryRepository) : InitializingBean {

    /**
     * Create query-entries index if not exists
     */
    override fun afterPropertiesSet() {
        this.queryEntryRepository.existIndex()
                .filter { exists -> !exists }
                .flatMap { this.queryEntryRepository.createIndex() }
                .block()
    }

}
