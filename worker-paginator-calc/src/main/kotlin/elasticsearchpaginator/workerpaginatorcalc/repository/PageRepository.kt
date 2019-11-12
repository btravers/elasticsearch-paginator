package elasticsearchpaginator.workerpaginatorcalc.repository

import com.fasterxml.jackson.databind.ObjectMapper
import elasticsearchpaginator.core.util.ElasticsearchUtils.NUMBER_REPLICAS_SETTING
import elasticsearchpaginator.core.util.ElasticsearchUtils.NUMBER_SHARDS_SETTING
import elasticsearchpaginator.core.util.ElasticsearchUtils.async
import elasticsearchpaginator.workerpaginatorcalc.configuration.ElasticsearchProperties
import elasticsearchpaginator.workerpaginatorcalc.model.Page
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.CreateIndexResponse
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.TermQueryBuilder
import org.elasticsearch.index.reindex.BulkByScrollResponse
import org.elasticsearch.index.reindex.DeleteByQueryRequest
import org.reactivestreams.Publisher
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.Resource
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.InputStreamReader

@Repository
class PageRepository(private val restHighLevelClient: RestHighLevelClient,
                     private val mapper: ObjectMapper,
                     private val elasticsearchProperties: ElasticsearchProperties,
                     @Value("classpath:pages-mappings.json") private val pagesMappings: Resource) {

    fun createIndex(): Mono<Void> {
        return Mono.fromCallable { InputStreamReader(this.pagesMappings.inputStream).readText() }
                .map { mappings ->
                    val settings = Settings.builder()
                            .put(NUMBER_SHARDS_SETTING, this.elasticsearchProperties.pagesIndexNumberShards)
                            .put(NUMBER_REPLICAS_SETTING, this.elasticsearchProperties.pagesIndexNumberReplicas)
                            .build()

                    CreateIndexRequest(this.elasticsearchProperties.pagesIndex)
                            .settings(settings)
                            .mapping(mappings, XContentType.JSON)
                }
                .flatMap { createIndexRequest ->
                    async<CreateIndexResponse> { actionListener ->
                        this.restHighLevelClient.indices().createAsync(createIndexRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .then()
    }

    fun existIndex(): Mono<Boolean> {
        return Mono.just(
                GetIndexRequest(this.elasticsearchProperties.pagesIndex)
        )
                .flatMap { getIndexRequest ->
                    async<Boolean> { actionListener ->
                        this.restHighLevelClient.indices().existsAsync(getIndexRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
    }

    fun save(pages: Publisher<Page>): Flux<Page> {
        return Flux.from(pages)
                .map { page ->
                    Pair(
                            IndexRequest(this.elasticsearchProperties.pagesIndex)
                                    .id("${page.queryId}_${page.page}")
                                    .source(this.mapper.writeValueAsBytes(page), XContentType.JSON),
                            page
                    )
                }
                .buffer(this.elasticsearchProperties.pagesBulkSize)
                .filter(List<Pair<IndexRequest, Page>>::isNotEmpty)
                .flatMap { indexRequests ->
                    val bulkRequest = BulkRequest().add(indexRequests.map(Pair<IndexRequest, Page>::first))
                    async<BulkResponse> { actionListener ->
                        this.restHighLevelClient.bulkAsync(bulkRequest, RequestOptions.DEFAULT, actionListener)
                    }
                            .thenMany(Flux.fromIterable(indexRequests.map(Pair<IndexRequest, Page>::second)))
                }
    }

    fun deleteByQueryId(id: String): Mono<Void> {
        return Mono.just(
                DeleteByQueryRequest(this.elasticsearchProperties.pagesIndex)
                        .setQuery(TermQueryBuilder("queryId", id))
        )
                .flatMap { deleteByQueryRequest ->
                    async<BulkByScrollResponse> { actionListener ->
                        this.restHighLevelClient.deleteByQueryAsync(deleteByQueryRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .then()
    }

}
