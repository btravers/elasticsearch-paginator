package elasticsearchpaginator.workerpaginatorcalc.model

import com.fasterxml.jackson.annotation.JsonRawValue

data class Page(
        val queryId: String,
        val page: Long,
        @get:JsonRawValue val searchAfterQueryParameters: String
)
