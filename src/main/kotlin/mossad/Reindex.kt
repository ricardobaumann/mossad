package mossad

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import java.io.InputStream
import java.io.OutputStream

private val baseUrl = System.getenv("apiBaseUrl")

class Reindex : RequestStreamHandler {
    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?) {
        //feedRecommendations()
        println("url is $baseUrl")
    }
}