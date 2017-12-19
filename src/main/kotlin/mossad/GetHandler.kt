package mossad

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.github.kittinunf.fuel.core.requests.write
import java.io.InputStream
import java.io.OutputStream

class GetHandler : RequestStreamHandler {
    override fun handleRequest(input: InputStream?, output: OutputStream?, context: Context?) {
        jedis["test"] = "test"
        val result = jedis["test"]
        output.write("Im running $result")
    }

}