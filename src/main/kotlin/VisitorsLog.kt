import com.fasterxml.jackson.databind.node.ArrayNode
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.jackson.responseObject
import com.github.kittinunf.result.Result
import redis.clients.jedis.Jedis
import java.util.*
import java.util.stream.Collectors

val pageSize = 500

val maxPages = 200

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

private val fromToIds = HashMap<String, MutableList<String>>()

private val jedis = Jedis("localhost", 32771)

fun main(args: Array<String>) {
    var page = 0
    loop@ while (page < maxPages) {
        try {
            println("Fetching page $page")
            val offset = page * pageSize
            val (_, _, result) = (visitorsLogPiwikUrl + "&filter_offset=$offset")
                    .httpGet().responseObject<ArrayNode>()
            when (result) {
                is Result.Failure -> {
                    println(result.error)
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
        page += 1
    }
    val pipeline = jedis.pipelined()
    fromToIds.entries.stream().forEach { t ->

        val mostHit = t.value
                .stream().collect(Collectors.groupingBy({ w -> w }, Collectors.counting()))
                .entries.stream()
                .sorted(Comparator.comparingInt<MutableMap.MutableEntry<Any?, Long>> { value -> value.value.toInt() }
                        .reversed())
                .limit(10).map { it.key }
                .collect(Collectors.toList())

        pipeline.set(t.key, mostHit.toJsonString())
        println("${t.key} = $mostHit")
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
                    val value = ids.stream().filter { it != key }.collect(Collectors.toList())
                    if (value.isNotEmpty()) {
                        fromToIds.getOrPut(key, { value }).addAll(value)
                    }
                }

            }
}
