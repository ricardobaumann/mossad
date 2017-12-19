package mossad

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.github.kittinunf.fuel.core.requests.write
import java.io.InputStream
import java.io.OutputStream
import java.util.*

data class ApiGatewayResponse(val body: String, val headers: Map<String, String> = Collections.singletonMap("Content-Type", "application/json"), val statusCode: Int = 200, val isBase64Encoded: Boolean = false)

class GetHandler : RequestStreamHandler {
    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?) {
        val inputJson = objectMapper.readTree(input)
        val id = inputJson["pathParameters"]["id"].asText()
        val result = jedis[id] ?: "[]"
        output.write(objectMapper.writeValueAsString(ApiGatewayResponse(body = result)))
    }

}