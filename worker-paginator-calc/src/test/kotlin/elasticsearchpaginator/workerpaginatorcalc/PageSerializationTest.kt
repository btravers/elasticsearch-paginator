package elasticsearchpaginator.workerpaginatorcalc

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import elasticsearchpaginator.workerpaginatorcalc.model.Page
import org.junit.jupiter.api.Test

class PageSerializationTest {

    private val mapper = jacksonObjectMapper()

    @Test
    fun `should correctly serialze a page`() {
        val page = Page(
                queryId = "id",
                page = 1,
                searchAfterQueryParameters = """{ "test": "test" }"""
        )
        println(mapper.writeValueAsString(page))
    }
    
}
