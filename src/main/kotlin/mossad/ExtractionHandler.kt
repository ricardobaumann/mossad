package mossad

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.github.kittinunf.fuel.httpPut
import com.github.kittinunf.result.Result
import java.io.InputStream
import java.io.OutputStream

class ExtractionHandler : RequestStreamHandler {
    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?) {
        val results = extractRecommendations()
        val sample = results.entries.stream().findFirst()
        if (sample.isPresent) {
            println("Try sample ${sample.get()}")

            val (_, _, result) = "$apiBaseUrl/recommendations".httpPut().body(objectMapper.writeValueAsString(results)).response()
            when (result) {
                is Result.Success -> {
                    println("Results processed and cached successfully")
                }
                is Result.Failure -> {
                    println("Failed due to  ${result.error}")
                }
            }
        } else {
            println("No results")
        }

    }
}