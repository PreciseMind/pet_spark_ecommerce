# Pet Spark E-Commerce application
## Tasks agreements
    1. User doesn't have cross open sessions, sessions open and close sequently
    2. Events appear in order timeline for each user
    3. A projection of purchase can't be not full, there isn't a purchase without session and not session without a purchase, skip not whole purchase projections
    4. Top 10 campaigns by revenue can include more then 10 for case when we have some campaigns with the same revenue
    5. Format data in json is incorrect ({{}} replace on {})
    6. Data keep inforamtion about 100,000 unique users, each user open aproximatly 10 sessions and make 5 actions, 30% of users make at the least one purchases (50,000 purchases)
## Configuration
With reference to an expected loading we will be process many little batch which relates with user's session. For successful processing we need to have maximum parallelism on spark.

**Hypothetical spark configs:**

    spark.serializer=org.apache.spark.serializer.KryoSerializer
    spark.driver.cores=4
    spark.driver.memory=8g
    spark.driver.memoryOverhead=780m
    spark.driver.maxResultSize=7g

    spark.default.parallelism=320
    spark.sql.shuffle.partitions=320

    spark.executor.cores=16
    spark.executor.memory=24g
    spark.executor.memoryOverhead=2g

    spark.dynamicAllocation.minExecutors=10
    spark.dynamicAllocation.maxExecutors=16
    spark.dynamicAllocation.enabled=true
    
Only reality will show effectivity the configuration :-)