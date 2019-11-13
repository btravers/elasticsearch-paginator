package elasticsearchpaginator.workerpaginator.model

import elasticsearchpaginator.core.model.Query
import java.time.Instant

data class QueryEntry(
        val query: Query,
        val lastUseDate: Instant,
        val lastComputationDate: Instant
)
