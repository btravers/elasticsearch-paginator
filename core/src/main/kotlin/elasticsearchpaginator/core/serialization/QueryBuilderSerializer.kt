package elasticsearchpaginator.core.serialization

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.SerializerProvider
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.elasticsearch.index.query.QueryBuilder

class QueryBuilderSerializer : StdSerializer<QueryBuilder>(QueryBuilder::class.java) {

    override fun serialize(queryBuilder: QueryBuilder, jsonGenerator: JsonGenerator, serializerProvider: SerializerProvider) {
        val json = queryBuilder.toString()
        jsonGenerator.writeRawValue(json)
    }

}
