import com.fasterxml.jackson.databind.node.ArrayNode
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.jackson.responseObject
import com.github.kittinunf.result.Result
import redis.clients.jedis.Jedis
import java.util.*
import java.util.stream.Collectors

val pageSize = 200

val visitorsLogPiwikUrl = "https://piwik-admin.up.welt.de/index.php?module=API&method=Live.getLastVisitsDetails&format=JSON&idSite=1&period=day&date=today&expanded=1&token_auth=325b6226f6b06472e78e6da694999486&filter_limit=$pageSize"

private fun String.extractContentId(): String {
    val parts = this.split("/")
    if (parts.size < 2) {
        return this
    }
    return parts[parts.size - 2].replace("article", "").replace("video", "")
}

private fun String.isNumeric(): Boolean {
    return this.toIntOrNull() != null
}

private val fromToIds = HashMap<String, MutableSet<String>>()

private val jedis = Jedis()

fun main(args: Array<String>) {
    var offset = 0
    loop@ while (true) {
        try {
            println("Fetching offset $offset")
            val (_, _, result) = (visitorsLogPiwikUrl + "&filter_offset=$offset")
                    .httpGet().responseObject<ArrayNode>()
            when (result) {
                is Result.Failure -> {
                    break@loop
                }
                is Result.Success -> {
                    val resultArray = result.get()
                    if (resultArray.any()) {
                        extractRelations(resultArray)
                    } else {
                        break@loop
                    }


                }
            }
        } catch (e: Exception) {
            println("Exiting due to $e")
            break@loop
        }
        offset += pageSize
    }
    val pipeline = jedis.pipelined()
    fromToIds.entries.stream().forEach { t ->
        println("${t.key} = ${t.value}")
        pipeline.set(t.key, t.value.toJsonString())
        println(jedis.get(t.key))
    }
    pipeline.sync()

}

private fun extractRelations(resultArray: ArrayNode) {
    resultArray.stream()
            .map { visit -> visit["actionDetails"] as ArrayNode }
            .map { actionDetails ->
                actionDetails.stream()
                        .map { actiondetail -> actiondetail["url"].asText() }
                        .collect(Collectors.toList())
            }
            .forEach { urls ->
                val ids = urls.stream()
                        .map { it.extractContentId() }
                        .filter { it.isNumeric() }.collect(Collectors.toList())

                ids.forEach { key ->
                    val value = ids.stream().filter { it != key }.collect(Collectors.toSet())
                    if (value.isNotEmpty()) {
                        fromToIds.getOrPut(key, { value }).addAll(value)
                    }
                }

            }
}
