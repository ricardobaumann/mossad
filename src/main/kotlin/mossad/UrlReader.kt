package mossad

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.fasterxml.jackson.databind.node.ArrayNode
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.jackson.responseObject
import com.github.kittinunf.result.Result

data class PiwikPageRequest(val url:String = "")

class UrlReader : RequestHandler<PiwikPageRequest,ArrayNode> {
    override fun handleRequest(input: PiwikPageRequest?, context: Context?): ArrayNode {
        val emptyResponse by lazy {objectMapper.createArrayNode()}
        try {
            val (_, _, result) = input!!.url
                    .httpGet().timeout(60000).timeoutRead(60000).responseObject<ArrayNode>()
            return when (result) {
                is Result.Failure -> {
                    println("Failed to read url due to ${result.error.response.statusCode}")
                    emptyResponse
                }
                is Result.Success -> {
                    println("Url processed successfully")
                    result.get()
                }
            }
        } catch (e: Exception) {
            println("Failed to process url: $e")
            return emptyResponse
        }
    }
}