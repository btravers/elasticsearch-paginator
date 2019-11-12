package elasticsearchpaginator.core.configuration

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import elasticsearchpaginator.core.model.SortBuilderList
import elasticsearchpaginator.core.serialization.QueryBuilderDeserializer
import elasticsearchpaginator.core.serialization.QueryBuilderSerializer
import elasticsearchpaginator.core.serialization.SortBuildersDeserializer
import elasticsearchpaginator.core.serialization.SortBuildersSerializer
import org.elasticsearch.index.query.QueryBuilder
import org.springframework.boot.autoconfigure.AutoConfigureBefore
import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
@AutoConfigureBefore(JacksonAutoConfiguration::class)
class JacksonConfiguration {

    @Bean
    fun mapper(): ObjectMapper {
        return OBJECT_MAPPER
    }

    companion object {
        val OBJECT_MAPPER = jacksonObjectMapper()
                .registerModule(JavaTimeModule())
                .apply {
                    val queryBuilderModule = SimpleModule()
                            .addDeserializer(QueryBuilder::class.java, QueryBuilderDeserializer())
                            .addSerializer(QueryBuilder::class.java, QueryBuilderSerializer())

                    this.registerModule(queryBuilderModule)
                }
                .apply {
                    val sortBuildersModule = SimpleModule()
                            .addDeserializer(SortBuilderList::class.java, SortBuildersDeserializer())
                            .addSerializer(SortBuilderList::class.java, SortBuildersSerializer())

                    this.registerModule(sortBuildersModule)
                }
    }

}
