import com.fasterxml.jackson.databind.ObjectMapper
import mu.KotlinLogging
import redis.clients.jedis.Jedis

val jedis by lazy { Jedis("localhost", 32771) }

val objectMapper = ObjectMapper()

val piwikBaseUrl = "https://piwik-admin.up.welt.de/index.php"

val logger = KotlinLogging.logger {}