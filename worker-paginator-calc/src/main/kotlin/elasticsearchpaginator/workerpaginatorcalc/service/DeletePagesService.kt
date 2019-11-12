package elasticsearchpaginator.workerpaginatorcalc.service

import elasticsearchpaginator.core.model.DeleteQuery
import elasticsearchpaginator.workerpaginatorcalc.repository.PageRepository
import elasticsearchpaginator.workerpaginatorcalc.transport.DeleteQuerySender
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class DeletePagesService(private val pageRepository: PageRepository,
                         private val deleteQuerySender: DeleteQuerySender) {

    fun deletePages(queryId: String): Mono<Void> {
        return Mono.just(queryId)
                .flatMap(this.pageRepository::deleteByQueryId)
                .thenReturn(
                        DeleteQuery(
                                queryId = queryId
                        )
                )
                .flatMap(this.deleteQuerySender::sendDeleteQueryEvent)
    }

}
