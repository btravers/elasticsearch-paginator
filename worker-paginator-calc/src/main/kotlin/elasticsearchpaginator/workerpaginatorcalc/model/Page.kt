package elasticsearchpaginator.workerpaginatorcalc.model

data class Page(
        val queryId: String,
        val page: Long,
        val searchAfterQueryParameters: Any
)
