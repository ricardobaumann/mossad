import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.jackson.responseObject
import com.github.kittinunf.result.Result
import java.util.*
import java.util.regex.Pattern
import java.util.stream.Stream
import java.util.stream.StreamSupport

private val piwikMostVisitedUrl: String = "https://piwik-admin.up.welt.de/index.php?module=API&method=Actions.getEntryPageUrls&format=JSON&idSite=1&period=day&date=today&expanded=1&token_auth=325b6226f6b06472e78e6da694999486&filter_limit=100"

data class Content(val section: String, val id: String, val visits: Int)

private val contentIdPattern: Pattern = Pattern.compile("([a-z]+[\\d]+)")

private fun JsonNode.flattenedSubtables(): Stream<JsonNode> {
    if (this.has("subtable")) {
        return Stream.concat(Stream.of(this),
                (this["subtable"] as ArrayNode).stream()
                        .flatMap(JsonNode::flattenedSubtables)
                        .peek({ t -> (t as ObjectNode).put("section", resolveSection(this)) }))
    }
    return Stream.of(this)
}

private fun resolveSection(t: JsonNode): String {
    return if (t.has("section")) {
        t["section"].asText()
    } else t["label"].asText()
}

private fun getMostVisited(): Stream<JsonNode> {

    val (_, _, result) = piwikMostVisitedUrl.httpGet().responseObject<ArrayNode>()
    when (result) {
        is Result.Failure -> {
            throw result.error
        }
        is Result.Success -> {
            return StreamSupport.stream(result.get().spliterator(), false)
        }
    }

}

fun main(args: Array<String>) {
    getMostVisited()
            .filter({ t -> t.has("subtable") })
            .map(JsonNode::flattenedSubtables)
            .flatMap { it }
            .filter({ t -> contentIdPattern.matcher(t["label"].asText()).matches() })
            .filter({ t -> t.has("section") })
            .map { t -> Content(t["section"].asText(), t["label"].asText(), t["nb_visits"].asInt()) }
            .sorted(Comparator.comparingInt(Content::visits).reversed())
            .forEach { t -> println(t) }
}


