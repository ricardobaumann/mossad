package mossad

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.fasterxml.jackson.core.type.TypeReference
import com.github.kittinunf.fuel.core.requests.write
import java.io.InputStream
import java.io.OutputStream

private val typeReference = object : TypeReference<Map<String, List<String>>>() {}

class PutHandler : RequestStreamHandler {
    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?) {
        val inputBody = objectMapper.readTree(input)["body"].asText()
        val results: Map<String, List<String>> = objectMapper.readValue(inputBody, typeReference)
        results.entries.forEach { t: Map.Entry<String, List<String>>? -> jedis[t!!.key] = t.value.toJsonString() }
        output.write(objectMapper.writeValueAsString(ApiGatewayResponse(body = "{}")))
    }

}