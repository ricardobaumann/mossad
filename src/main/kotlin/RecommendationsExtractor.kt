import com.fasterxml.jackson.databind.node.ArrayNode
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.jackson.responseObject
import com.github.kittinunf.result.Result
import java.util.*
import java.util.stream.Collectors

private val pageSize = 500

private val maxPages = 10

private val visitorsLogPiwikUrl = "$piwikBaseUrl?module=API&method=Live.getLastVisitsDetails&format=JSON&idSite=1&period=day&date=today&expanded=1&token_auth=325b6226f6b06472e78e6da694999486&filter_limit=$pageSize"

private fun String.extractContentId(): String {
    val parts = this.split("/")
    if (parts.size < 2) {
        return this
    }
    return parts[parts.size - 2].replace("article", "").replace("video", "")
}


fun main(args: Array<String>) {
    feedRecommendations()
}

fun feedRecommendations() {
    val fromToIds = HashMap<String, MutableList<String>>()
    var page = 0
    loop@ while (page < maxPages) {
        try {
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
                        extractRelations(resultArray, fromToIds)
                    } else {
                        break@loop
                    }


                }
            }
        } catch (e: Exception) {
            logger.error(e) { "Exiting due to $e" }
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

private fun extractRelations(resultArray: ArrayNode, fromToIds: HashMap<String, MutableList<String>>) {
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
