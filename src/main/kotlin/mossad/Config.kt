package mossad

import com.fasterxml.jackson.databind.ObjectMapper
import redis.clients.jedis.Jedis
import java.util.*

val jedis by lazy { Jedis(System.getenv("redisEndpoint") ?: "localhost", (System.getenv("redisPort") ?: "6379").toInt()) }

val objectMapper = ObjectMapper()

val piwikBaseUrl = "https://piwik-admin.up.welt.de/index.php"

val apiBaseUrl = System.getenv("apiBaseUrl") ?: ""

data class ApiGatewayResponse(val body: String, val headers: Map<String, String> = Collections.singletonMap("Content-Type", "application/json"), val statusCode: Int = 200, val isBase64Encoded: Boolean = false)

data class Params(val piwikToken: String = "")

val params = objectMapper.readValue(Thread.currentThread().contextClassLoader.getResourceAsStream("application.json"), Params::class.java)!!