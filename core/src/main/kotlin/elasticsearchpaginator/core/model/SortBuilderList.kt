package elasticsearchpaginator.core.model

import org.elasticsearch.search.sort.SortBuilder

class SortBuilderList(private val innerList: List<SortBuilder<*>>) : List<SortBuilder<*>> by ArrayList<SortBuilder<*>>(innerList)
