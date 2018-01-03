package mossad

import com.fasterxml.jackson.databind.node.ArrayNode
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.jackson.responseObject
import com.github.kittinunf.result.Result
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.function.Supplier
import kotlin.collections.HashMap

enum class IndexMode {
    ID, ID_AND_DEVICE
}


private fun String.asArticleId(): String {
    val parts = this.split("/")
    if (parts.size < 2) {
        return this
    }
    return parts[parts.size - 2].replace("article", "").replace("video", "")
}


fun main(args: Array<String>) {
    val start = System.currentTimeMillis()
    val result = extractRecommendations(ExtractionContext())
    result.entries.forEach { println("${it.key} = ${it.value}") }
    println("Finished with ${System.currentTimeMillis()-start} millis")
}

fun extractRecommendations(extractionContext: ExtractionContext): Map<String, List<String>> {
    val pageSize = extractionContext.pageSize
    val maxPages = extractionContext.maxPagesPerCall
    val visitorsLogPiwikUrl = "$piwikBaseUrl?module=API&method=Live.getLastVisitsDetails&format=JSON" +
            "&idSite=1&period=day&date=today&expanded=1&filter_sort_column=lastActionTimestamp&filter_sort_order=desc" +
            "&showColumns=actionDetails,deviceType&token_auth=${params.piwikToken}&filter_limit=$pageSize"

    val threadPool = Executors.newFixedThreadPool(extractionContext.threadPoolSize)
    val futures = buildPiwikFutureCalls(extractionContext.currentPage, maxPages, threadPool, visitorsLogPiwikUrl, pageSize)

    CompletableFuture.allOf(*futures.toTypedArray()).get()
    threadPool.shutdown()
    val results = futures.map { it.join() }
    return if (extractionContext.indexMode == IndexMode.ID) results.extractByIdRelations() else results.extractByIdAndDeviceRelations()
    //return map.entries.associate { it.key to extractMostHit(it.value) }

}

private fun List<ArrayNode>.extractByIdAndDeviceRelations(): HashMap<String, MutableList<String>> {
    val fromToIds = HashMap<String, MutableList<String>>()
    this.flatMap { it }.map {
        Pair<String,List<String>>(it["deviceType"].asText(), (it["actionDetails"] as ArrayNode)
                .map { it["url"].asText().asArticleId() }
                .filter { it.isNumeric() }) }
            .filter { it.second.size > 1 }
            .forEach { it.second.forEach { item -> fromToIds.getOrPut("$item-${it.first}",{ mutableListOf() }).addAll(it.second.filter { id -> id!=item }) } }

    return fromToIds
}

private fun buildPiwikFutureCalls(currentPage: Int, maxPages: Int, threadPool: ExecutorService, piwikUrl: String, pageSize: Int): List<CompletableFuture<ArrayNode>> {
    val emptyResponse by lazy {objectMapper.createArrayNode()}

    return (currentPage..maxPages).map { page ->
        CompletableFuture.supplyAsync(Supplier {
            try {
                println("Starting page $page")
                val offset = (page-1) * pageSize
                val (_, _, result) = ("$piwikUrl&filter_offset=$offset")
                        .httpGet().timeout(60000).timeoutRead(60000).responseObject<ArrayNode>()
                when (result) {
                    is Result.Failure -> {
                        println("Failed to read page $page due to ${result.error.response.statusCode}")
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

    }
}

private fun List<ArrayNode>.extractByIdRelations(): HashMap<String, MutableList<String>> {
    val fromToIds = HashMap<String, MutableList<String>>()
    this.flatMap { it }.map { visit -> visit["actionDetails"] as ArrayNode }
            .map { actionDetails ->
                actionDetails
                        .map { actionDetail -> actionDetail["url"].asText() }
            }
            .forEach { urls ->
                val ids = urls
                        .map { it.asArticleId() }
                        .filter { it.isNumeric() }

                ids.forEach { key ->
                    val value = ids.filter { it != key }
                    if (value.isNotEmpty()) {
                        fromToIds.getOrPut(key, { mutableListOf() }).addAll(value)
                    }
                }

            }
    return fromToIds
}
