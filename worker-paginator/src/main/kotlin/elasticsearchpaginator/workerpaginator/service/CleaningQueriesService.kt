package elasticsearchpaginator.workerpaginator.service

import elasticsearchpaginator.core.model.DeletePages
import elasticsearchpaginator.workerpaginator.repository.QueryEntryRepository
import elasticsearchpaginator.workerpaginator.transport.DeletePagesSender
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant

@Service
class CleaningQueriesService(private val queryEntryRepository: QueryEntryRepository,
                             private val deletePagesSender: DeletePagesSender,
                             @Value("\${app.query-entries-ttl}") private val queryEntriesTtl: Duration) {

    fun deleteQuery(queryId: String): Mono<Void> {
        return this.queryEntryRepository.deleteOne(queryId)
    }

    fun getOutdatedQueriesThenDeleteRelatedPages(): Mono<Void> {
        return Mono.just(Instant.now().minus(this.queryEntriesTtl))
                .flatMapMany(this.queryEntryRepository::findAllWithLastUseDateOlderThan)
                .map { queryEntry ->
                    DeletePages(
                        queryId = queryEntry.query.hash()
                )
                }
                .flatMap(this.deletePagesSender::sendDeletePagesEvent)
                .then()
    }

}
