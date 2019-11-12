package elasticsearchpaginator.core.serialization

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import elasticsearchpaginator.core.model.SortBuilderList

class SortBuildersSerializer : StdSerializer<SortBuilderList>(SortBuilderList::class.java) {

    override fun serialize(sortBuilders: SortBuilderList, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider) {
        jsonGenerator.writeStartArray()
        sortBuilders.map { sortBuilder -> jsonGenerator.writeRawValue(sortBuilder.toString()) }
        jsonGenerator.writeEndArray()
    }

}
