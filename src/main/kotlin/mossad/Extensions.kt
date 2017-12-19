package mossad

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import java.util.stream.Stream
import java.util.stream.StreamSupport

fun ArrayNode.stream(): Stream<JsonNode> {
    return StreamSupport.stream(this.spliterator(), false)
}

fun <E> MutableList<E>.toJsonString(): String {
    return objectMapper.writeValueAsString(this)
}

fun String.isNumeric(): Boolean {
    return this.toIntOrNull() != null
}