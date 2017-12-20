package mossad

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.github.kittinunf.fuel.httpPut
import com.github.kittinunf.result.Result
import java.io.InputStream
import java.io.OutputStream

private val baseUrl = System.getenv("apiBaseUrl")

class Reindex : RequestStreamHandler {
    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?) {
        val results = feedRecommendations()
        val (_, _, result) = "$apiBaseUrl/recommendations".httpPut().body(objectMapper.writeValueAsString(results)).response()
        when (result) {
            is Result.Success -> {
                println("Results processed and cached successfully")
            }
            is Result.Failure -> {
                println("Failed due to  ${result.error}")
            }
        }
    }
}