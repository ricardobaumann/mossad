package mossad

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.module.kotlin.readValue
import com.github.kittinunf.fuel.httpGet
import com.github.kittinunf.fuel.jackson.responseObject
import com.github.kittinunf.result.Result
import java.io.InputStream
import java.io.OutputStream

data class UrlRequest(val url:String = "")

class UrlReader : RequestStreamHandler {
    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?) {
        val urlRequest = objectMapper.readValue<UrlRequest>(input!!)
        println("Incoming url is ${urlRequest.url}")
        val (_, _, result) = urlRequest.url
                .httpGet().timeout(60000).timeoutRead(60000).responseObject<JsonNode>()
        when (result) {
            is Result.Failure<*, *> -> {
                throw result.error
            }
            is Result.Success -> {
                println("Url processed successfully")
                objectMapper.writeValue(output,result.get())
            }
        }
    }

}