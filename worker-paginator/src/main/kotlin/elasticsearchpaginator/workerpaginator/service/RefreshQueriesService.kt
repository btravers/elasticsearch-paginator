package elasticsearchpaginator.workerpaginator.service

import elasticsearchpaginator.workerpaginator.model.QueryEntry
import elasticsearchpaginator.workerpaginator.repository.QueryEntryRepository
import elasticsearchpaginator.workerpaginator.transport.ComputePagesSender
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class RefreshQueriesService(private val queryEntryRepository: QueryEntryRepository,
                            private val computePagesSender: ComputePagesSender) {

    fun refreshPagesForAllQueries(): Mono<Void> {
        return this.queryEntryRepository.findAll()
                .map(QueryEntry::query)
                .flatMap(this.computePagesSender::sendComputePagesEvent)
                .then()
    }

}
