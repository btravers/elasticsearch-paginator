package elasticsearchpaginator.workerpaginatorcalc.configuration

import elasticsearchpaginator.workerpaginatorcalc.repository.PageRepository
import org.springframework.beans.factory.InitializingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Configuration

@Configuration
@EnableConfigurationProperties(ElasticsearchProperties::class)
class ElasticsearchConfiguration(private val pageRepository: PageRepository) : InitializingBean {

    /**
     * Create pages index if not exists
     */
    override fun afterPropertiesSet() {
        this.pageRepository.existIndex()
                .filter { exists -> !exists }
                .flatMap { this.pageRepository.createIndex() }
                .block()
    }

}
