import spark.Spark.get


fun main(args: Array<String>) {
    get("/:id/recommendations") { req, _ -> jedis.get(req.params("id")) }
}