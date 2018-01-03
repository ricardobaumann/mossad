package mossad

import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder
import com.amazonaws.services.lambda.model.InvokeRequest
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.kittinunf.fuel.httpPut
import com.github.kittinunf.result.Result
import java.io.InputStream
import java.io.OutputStream

data class ExtractionContext(val currentPage:Int = 1,
                             val maxPages:Int = 100,
                             val pageSize:Int = 500,
                             val threadPoolSize: Int = 5,
                             val maxPagesPerCall: Int = 10,
                             val indexMode: IndexMode = IndexMode.ID,
                             val resultsSoFar: Map<String,List<String>> = mapOf())

class ExtractionHandler : RequestStreamHandler {
    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?) {
        val extractionContext = try { objectMapper.readValue<ExtractionContext>(input!!) } catch (e: Exception) {ExtractionContext()}
        println("Processing context: ${extractionContext.currentPage}")
        if (extractionContext.currentPage > extractionContext.maxPages) {
            val sample = extractionContext.resultsSoFar.entries.first()
                println("Try sample $sample")

                val (_, _, result) = "$apiBaseUrl/recommendations".httpPut()
                        .body(objectMapper.writeValueAsString(extractionContext.resultsSoFar.entries.associate { it.key to extractMostHit(it.value) })).response()
                when (result) {
                    is Result.Success -> {
                        println("Results processed and cached successfully")
                    }
                    is Result.Failure -> {
                        println("Failed due to  ${result.error}")
                    }
                }

        } else {
            val results = extractRecommendations(extractionContext)
            val nextContext = ExtractionContext(extractionContext.currentPage+extractionContext.maxPagesPerCall,
                    extractionContext.maxPages,
                    extractionContext.pageSize,
                    extractionContext.threadPoolSize,
                    extractionContext.maxPagesPerCall,
                    extractionContext.indexMode,
                    extractionContext.resultsSoFar.toMutableMap().apply { results.forEach { k, v -> put(k,getOrDefault(k, listOf()).plus(v)) } })
            val client = AWSLambdaAsyncClientBuilder.standard().withRegion(System.getenv("region")).build()
            val invokeRequest = InvokeRequest().withPayload(objectMapper.writeValueAsString(nextContext)).withFunctionName(System.getenv("functionName"))
            try {client.invoke(invokeRequest)} catch (e: Exception) {println("Failed to wait, but dont worry!")}
        }



    }

    private fun extractMostHit(inputList: List<String>): List<String> {
        return inputList.groupingBy { it }.eachCount()
                .entries.sortedBy { it.value }.reversed()
                .take(10)
                .map { it.key }

    }

}


