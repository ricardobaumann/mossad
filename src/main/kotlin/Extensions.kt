import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import java.util.stream.Stream
import java.util.stream.StreamSupport

fun ArrayNode.stream(): Stream<JsonNode> {
    return StreamSupport.stream(this.spliterator(), false)
}

private val objectMapper = ObjectMapper()

fun <E> MutableList<E>.toJsonString(): String {
    return objectMapper.writeValueAsString(this)
}