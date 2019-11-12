package elasticsearchpaginator.workerpaginator.service

import elasticsearchpaginator.core.model.Query
import elasticsearchpaginator.workerpaginator.model.QueryEntry
import elasticsearchpaginator.workerpaginator.repository.QueryEntryRepository
import elasticsearchpaginator.workerpaginator.transport.ComputePagesSender
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant

@Component
class QueriesService(private val queryEntryRepository: QueryEntryRepository,
                     private val computePagesSender: ComputePagesSender,
                     @Value("\${app.min-interval-between-pages-refresh}") private val minIntervalBetweenPagesRefresh: Duration) {

    fun upsertQueryAndAskForPagesComputation(query: Query): Mono<Void> {
        return this.queryEntryRepository.findOne(query.hash())
                .defaultIfEmpty(
                        QueryEntry(
                                query = query,
                                lastUseDate = Instant.now()
                        )
                )
                .flatMap(this.queryEntryRepository::updateLastUseDate)
                .filter { queryEntry ->
                    Instant.now().minus(minIntervalBetweenPagesRefresh) > queryEntry.lastComputationDate
                }
                .flatMapMany { queryEntry ->
                    this.computePagesSender.sendComputePagesEvent(query)
                            .flatMap { this.queryEntryRepository.updateLastComputationDate(queryEntry.copy(lastComputationDate = Instant.now())) }
                }
                .then()
    }

}
