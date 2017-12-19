package mossad

import com.fasterxml.jackson.databind.ObjectMapper
import redis.clients.jedis.Jedis

//val jedis by lazy { Jedis("localhost", 32771) }
val jedis by lazy { Jedis("mos-se-1o51np5j4txno.r2wjf6.0001.euw1.cache.amazonaws.com", 6379) }
//val jedis by lazy { Jedis("mossad-test.r2wjf6.0001.euw1.cache.amazonaws.com:6379", 6379) }
//mossad-test.r2wjf6.0001.euw1.cache.amazonaws.com:6379

val objectMapper = ObjectMapper()

val piwikBaseUrl = "https://piwik-admin.up.welt.de/index.php"