package mossad

import com.fasterxml.jackson.databind.ObjectMapper
import redis.clients.jedis.Jedis
import java.util.*

val jedis by lazy { Jedis("mos-se-1o51np5j4txno.r2wjf6.0001.euw1.cache.amazonaws.com", 6379) }
//val jedis by lazy { Jedis("localhost", 32771) }

val objectMapper = ObjectMapper()

val piwikBaseUrl = "https://piwik-admin.up.welt.de/index.php"

val apiBaseUrl = System.getenv("apiBaseUrl")

data class ApiGatewayResponse(val body: String, val headers: Map<String, String> = Collections.singletonMap("Content-Type", "application/json"), val statusCode: Int = 200, val isBase64Encoded: Boolean = false)