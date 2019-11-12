package elasticsearchpaginator.workerpaginatorcalc.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import elasticsearchpaginator.core.model.Query
import elasticsearchpaginator.workerpaginatorcalc.model.Page
import elasticsearchpaginator.workerpaginatorcalc.repository.EntityElasticsearchRepository
import elasticsearchpaginator.workerpaginatorcalc.repository.PageRepository
import org.elasticsearch.action.search.SearchResponse
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Service
class ComputePagesService(private val pageRepository: PageRepository,
                          private val mapper: ObjectMapper,
                          private val entityElasticsearchRepository: EntityElasticsearchRepository) {

    fun computePages(query: Query): Mono<Void> {
        return this.getLastEntityForEachPage(query)
                .index()
                .map { indexAndEntity ->
                    Page(
                            queryId = query.hash(),
                            page = indexAndEntity.t1 + 1,
                            searchAfterQueryParameters = indexAndEntity.t2
                    )
                }
                .transform(this.pageRepository::save)
                .then()
    }

    private fun getLastEntityForEachPage(query: Query): Flux<Any> {
        return this.findAllEntitiesWithTotalHits(query)
                .index()
                .filter { indexAndEntity ->
                    this.matchesALastPosition(
                            index = indexAndEntity.t1,
                            firstPageSize = query.firstPageSize ?: query.size,
                            size = query.size,
                            totalHits =indexAndEntity.t2.second
                    )
                }
                .map { indexAndEnity -> indexAndEnity.t2.first }
    }

    private fun findAllEntitiesWithTotalHits(query: Query): Flux<Pair<Any, Long>> {
        return this.entityElasticsearchRepository.searchScroll(query.index, query.query, query.sort, query.size)
                .expand { searchResponse ->
                    if (searchResponse.hits.hits.isEmpty()) {
                        this.entityElasticsearchRepository.clearScroll(searchResponse.scrollId)
                                .then(Mono.empty<SearchResponse>())
                    } else {
                        this.entityElasticsearchRepository.scroll(searchResponse.scrollId)
                    }
                }
                .flatMapIterable { searchResponse ->
                    searchResponse.hits.map { searchHit ->
                        Pair(this.mapper.readValue<Any>(searchHit.sourceRef.streamInput()), searchResponse.hits.totalHits!!.value)
                    }
                }
    }

    private fun matchesALastPosition(index: Long, firstPageSize: Int, size: Int, totalHits: Long): Boolean {
        return index == firstPageSize - 1L || (index - firstPageSize + 1L) % size == 0L && index != totalHits - 1L
    }

}
