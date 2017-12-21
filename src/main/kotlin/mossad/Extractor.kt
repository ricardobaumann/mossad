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
import java.util.stream.Stream
import kotlin.system.exitProcess

private val pageSize = (System.getenv("pageSize") ?: "500").toInt()

private val maxPages = (System.getenv("maxPages") ?: "10").toInt()

private val visitorsLogPiwikUrl = "$piwikBaseUrl?module=API&method=Live.getLastVisitsDetails&format=JSON&idSite=1&period=day&date=today&expanded=1&token_auth=${params.piwikToken}&filter_limit=$pageSize"

private fun String.extractContentId(): String {
    val parts = this.split("/")
    if (parts.size < 2) {
        return this
    }
    return parts[parts.size - 2].replace("article", "").replace("video", "")
}


fun main(args: Array<String>) {
    println("Started at ${LocalDateTime.now()}")
    extractRecommendations()
    println("Finished at ${LocalDateTime.now()}")
}

fun extractRecommendations(): Map<String, MutableList<String>> {
    val threadPool = Executors.newFixedThreadPool((System.getenv("threadPoolSize") ?: "10").toInt())
    val emptyResponse = objectMapper.createArrayNode()
    val futures = IntStream.rangeClosed(0, maxPages).mapToObj { page ->
        CompletableFuture.supplyAsync(Supplier {
            try {
                println("Starting page $page")
                val offset = page * pageSize
                val (_, _, result) = ("$visitorsLogPiwikUrl&filter_offset=$offset")
                        .httpGet().timeout(60000).timeoutRead(60000).responseObject<ArrayNode>()
                when (result) {
                    is Result.Failure -> {
                        println("Failed to read piwik results due to ${result.error.response.statusCode}")
                        emptyResponse
                    }
                    is Result.Success -> {
                        println("Page $page processed successfully")
                        result.get()
                    }
                }
            } catch (e: Exception) {
                println("Failed to process page $page: $e")
                emptyResponse
            }

        }, threadPool)

    }.collect(Collectors.toList())

    CompletableFuture.allOf(*futures.toTypedArray()).get()
    threadPool.shutdown()
    return futures.stream().map { it.join() }.extractRelations().entries.stream().collect(Collectors.toList()).associate { it.key to extractMostHit(it.value) }

}

private fun Stream<ArrayNode>.extractRelations(): HashMap<String, MutableList<String>> {
    val fromToIds = HashMap<String, MutableList<String>>()
    this.flatMap { it.stream() }.map { visit -> visit["actionDetails"] as ArrayNode }
            .map { actionDetails ->
                actionDetails.stream()
                        .map { actionDetail -> actionDetail["url"].asText() }
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
    //fromToIds.entries.forEach { println("${it.key} = ${it.value}") }
    return fromToIds
}

fun extractMostHit(inputList: MutableList<String>): MutableList<String> {

    return inputList.stream().collect(Collectors.groupingBy({ w -> w }, Collectors.counting()))
            .entries.stream()
            .sorted(Comparator.comparingInt<MutableMap.MutableEntry<Any?, Long>> { value -> value.value.toInt() }
                    .reversed())
            .limit(10).map { it.key }
            .map { it.toString() }
            .collect(Collectors.toList())

}
