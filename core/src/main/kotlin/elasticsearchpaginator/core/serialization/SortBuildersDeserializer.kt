package elasticsearchpaginator.core.serialization

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import elasticsearchpaginator.core.model.SortBuilderList
import org.elasticsearch.common.xcontent.DeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.search.sort.FieldSortBuilder

class SortBuildersDeserializer : StdDeserializer<SortBuilderList>(SortBuilderList::class.java) {

    override fun deserialize(jsonParser: JsonParser, deserializerContext: DeserializationContext): SortBuilderList {
        val raw = jsonParser.codec.readTree<JsonNode>(jsonParser).toString()
        return SortBuilderList(FieldSortBuilder.fromXContent(XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, raw)))
    }

}
