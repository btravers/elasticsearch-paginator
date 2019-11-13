package elasticsearchpaginator.workerpaginator.repository

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import elasticsearchpaginator.core.util.ElasticsearchUtils.NUMBER_REPLICAS_SETTING
import elasticsearchpaginator.core.util.ElasticsearchUtils.NUMBER_SHARDS_SETTING
import elasticsearchpaginator.core.util.ElasticsearchUtils.async
import elasticsearchpaginator.workerpaginator.configuration.ElasticsearchProperties
import elasticsearchpaginator.workerpaginator.model.QueryEntry
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.indices.CreateIndexRequest
import org.elasticsearch.client.indices.CreateIndexResponse
import org.elasticsearch.client.indices.GetIndexRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.io.Resource
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.InputStreamReader
import java.time.Instant

@Repository
class QueryEntryRepository(private val restHighLevelClient: RestHighLevelClient,
                           private val mapper: ObjectMapper,
                           private val elasticsearchProperties: ElasticsearchProperties,
                           @Value("classpath:query-entries-mappings.json") private val queryEntriesMappings: Resource) {

    private val elasticsearchMaxResultWindow = 10000

    fun createIndex(): Mono<Void> {
        return Mono.fromCallable { InputStreamReader(this.queryEntriesMappings.inputStream).readText() }
                .map { mappings ->
                    val settings = Settings.builder()
                            .put(NUMBER_SHARDS_SETTING, this.elasticsearchProperties.queryEntriesIndexNumberShards)
                            .put(NUMBER_REPLICAS_SETTING, this.elasticsearchProperties.queryEntriesIndexNumberReplicas)
                            .build()

                    CreateIndexRequest(this.elasticsearchProperties.queryEntriesIndex)
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
                GetIndexRequest(this.elasticsearchProperties.queryEntriesIndex)
        )
                .flatMap { getIndexRequest ->
                    async<Boolean> { actionListener ->
                        this.restHighLevelClient.indices().existsAsync(getIndexRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
    }

    fun updateLastUseDate(queryEntry: QueryEntry): Mono<Void> {
        return Mono.just(
                UpdateRequest()
                        .index(this.elasticsearchProperties.queryEntriesIndex)
                        .id(queryEntry.query.hash())
                        .doc(this.mapper.writeValueAsBytes(QueryEntryLastUseDateUpdate(lastUseDate = queryEntry.lastUseDate)), XContentType.JSON)
                        .upsert(this.mapper.writeValueAsBytes(queryEntry), XContentType.JSON)
        )
                .flatMap { updateRequest ->
                    async<UpdateResponse> { actionListener ->
                        this.restHighLevelClient.updateAsync(updateRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .then()
    }

    fun updateLastComputationDate(queryEntry: QueryEntry): Mono<Void> {
        return Mono.just(
                UpdateRequest()
                        .index(this.elasticsearchProperties.queryEntriesIndex)
                        .id(queryEntry.query.hash())
                        .doc(this.mapper.writeValueAsBytes(QueryEntryLastComputationDateUpdate(lastComputationDate = queryEntry.lastComputationDate)), XContentType.JSON)
        )
                .flatMap { updateRequest ->
                    async<UpdateResponse> { actionListener ->
                        this.restHighLevelClient.updateAsync(updateRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .then()
    }

    fun findOne(id: String): Mono<QueryEntry> {
        return Mono.just(
                GetRequest()
                        .index(this.elasticsearchProperties.queryEntriesIndex)
                        .id(id)
        )
                .flatMap { getRequest ->
                    async<GetResponse> { actionListener ->
                        this.restHighLevelClient.getAsync(getRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .filter { getResponse ->
                    getResponse.isExists
                }
                .map { getResponse ->
                    this.mapper.readValue<QueryEntry>(getResponse.sourceAsBytes)
                }
    }

    // TODO use scroll queries instead of limiting to the max result window
    fun findAll(): Flux<QueryEntry> {
        return Mono.just(
                SearchSourceBuilder()
                        .query(QueryBuilders.matchAllQuery())
                        .size(elasticsearchMaxResultWindow)
        )
                .map { searchSourceBuilder ->
                    SearchRequest()
                            .indices(this.elasticsearchProperties.queryEntriesIndex)
                            .source(searchSourceBuilder)
                }
                .flatMap { searchRequest ->
                    async<SearchResponse> { actionListener ->
                        this.restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .flatMapIterable { searchResponse ->
                    searchResponse.hits
                }
                .map { searchHit ->
                    this.mapper.readValue<QueryEntry>(searchHit.sourceRef.streamInput())
                }
    }

    // TODO use scroll queries instead of limiting to the max result window
    fun findAllWithLastUseDateOlderThan(instant: Instant): Flux<QueryEntry> {
        return Mono.just(
                SearchSourceBuilder()
                        .query(QueryBuilders.rangeQuery("lastUseDate").lte(instant.toEpochMilli()))
                        .size(elasticsearchMaxResultWindow)
        )
                .map { searchSourceBuilder ->
                    SearchRequest()
                            .indices(this.elasticsearchProperties.queryEntriesIndex)
                            .source(searchSourceBuilder)
                }
                .flatMap { searchRequest ->
                    async<SearchResponse> { actionListener ->
                        this.restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .flatMapIterable { searchResponse ->
                    searchResponse.hits
                }
                .map { searchHit ->
                    this.mapper.readValue<QueryEntry>(searchHit.sourceRef.streamInput())
                }
    }

    fun deleteOne(id: String): Mono<Void> {
        return Mono.just(
                DeleteRequest()
                        .index(this.elasticsearchProperties.queryEntriesIndex)
                        .id(id)
        )
                .flatMap { deleteRequest ->
                    async<DeleteResponse> { actionListener ->
                        this.restHighLevelClient.deleteAsync(deleteRequest, RequestOptions.DEFAULT, actionListener)
                    }
                }
                .then()
    }

    private data class QueryEntryLastUseDateUpdate(
            val lastUseDate: Instant
    )

    private data class QueryEntryLastComputationDateUpdate(
            val lastComputationDate: Instant
    )

}
