package elasticsearchpaginator.core.serialization

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationContext
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders

class QueryBuilderDeserializer : StdDeserializer<QueryBuilder>(QueryBuilder::class.java) {

    override fun deserialize(jsonParser: JsonParser, deserializationContext: DeserializationContext): QueryBuilder {
        val raw = jsonParser.codec.readTree<JsonNode>(jsonParser).toString()
        return QueryBuilders.wrapperQuery(raw)
    }

}
