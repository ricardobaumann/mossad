package mossad

import com.fasterxml.jackson.databind.node.ArrayNode
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.jackson.responseObject
import com.github.kittinunf.result.Result
import java.time.LocalDateTime
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.function.Supplier
import java.util.stream.Collectors
import java.util.stream.IntStream
import kotlin.system.exitProcess

private val pageSize = 500

private val maxPages = 30

private val visitorsLogPiwikUrl = "$piwikBaseUrl?module=API&method=Live.getLastVisitsDetails&format=JSON&idSite=1&period=day&date=today&expanded=1&token_auth=325b6226f6b06472e78e6da694999486&filter_limit=${pageSize}"

private val threadPool = Executors.newFixedThreadPool(maxPages)

private fun String.extractContentId(): String {
    val parts = this.split("/")
    if (parts.size < 2) {
        return this
    }
    return parts[parts.size - 2].replace("article", "").replace("video", "")
}


fun main(args: Array<String>) {
    println("Started at ${LocalDateTime.now()}")
    feedRecommendations()
    println("Finished at ${LocalDateTime.now()}")
    exitProcess(0)
}

fun feedRecommendations() {
    val fromToIds = HashMap<String, MutableList<String>>()
    val emptyResponse = objectMapper.createArrayNode()
    IntStream.rangeClosed(0, maxPages).mapToObj { page ->
        CompletableFuture.supplyAsync(Supplier {
            try {
                val offset = page * pageSize
                val (_, _, result) = ("$visitorsLogPiwikUrl&filter_offset=$offset")
                        .httpGet().responseObject<ArrayNode>()
                when (result) {
                    is Result.Failure -> {
                        println("Failed to read piwik results due to ${result.error}")
                        emptyResponse
                    }
                    is Result.Success -> {
                        println("Page $page processed successfully")
                        result.get()


                    }
                }
            } catch (e: Exception) {
                println("Failed to process page: $e")
                emptyResponse
            }

        }, threadPool)

    }.map { it.join() }.forEach { extractRelations(it, fromToIds) }
    //threadPool.awaitTermination(timeoutInMillis.toLong(), TimeUnit.MILLISECONDS)
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
