package elasticsearchpaginator.workerpaginator.web

import elasticsearchpaginator.workerpaginator.service.CleaningQueriesService
import elasticsearchpaginator.workerpaginator.service.RefreshQueriesService
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/queries")
class QueriesController(private val cleaningQueriesService: CleaningQueriesService,
                        private val refreshQueriesService: RefreshQueriesService) {

    @GetMapping("/clear")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun clearOutdatedQueries(): Mono<Void> {
        return this.cleaningQueriesService.getOutdatedQueriesThenDeleteRelatedPages()
    }

    @GetMapping("/refresh")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    fun refreshAllQueries(): Mono<Void> {
        return this.refreshQueriesService.refreshPagesForAllQueries()
    }

}
