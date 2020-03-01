package com.precisemind.pet.ecommerce.builder.impl

import com.precisemind.pet.ecommerce.builder.PurchaseProjectionBuilder
import com.precisemind.pet.ecommerce.model.EventType
import org.apache.spark.sql.functions.{expr, get_json_object, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object PurchaseProjectionBuilderSQL extends PurchaseProjectionBuilder{

  /**
    * Make projections of purchases
    * The method executes a full cycle of computations -
    * reading a csv data (click events, purchases), calculating sessions of users, merging sessions with purchases that to get projections of purchases
    */
  def process(clicksPath: String, purchasesPath: String)(implicit spark: SparkSession): DataFrame = {

    val clicks = PurchaseProjectionBuilderSQL.buildSourceDF(clicksPath)

    val sessions = PurchaseProjectionBuilderSQL.buildSessionsDF(clicks)

    val purchases = PurchaseProjectionBuilderSQL.buildPurchasesDF(purchasesPath)

    val projectedPurchases = PurchaseProjectionBuilderSQL.buildProjectedPurchasesDF(purchases, sessions)
    projectedPurchases
  }

  /**
    * Read data of click events from csv file
    */
  def buildSourceDF(pathClicksFile: String)(implicit spark: SparkSession): DataFrame = {
    val clicks = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("escape", "\"")
      .load(pathClicksFile)


    buildSourceDF(clicks)
  }

  /**
    * Supportive method for building source, use for test
    */
  def buildSourceDFTest(testDS: DataFrame)(implicit spark: SparkSession): DataFrame = {
    buildSourceDF(testDS)
  }

  /**
    * Make sessions of users: first step - forming start and end time for app_open and app_close events
    * second step - merge events of purchase type with formed session frames and get ready sessions
    */
  def buildSessionsDF(source: DataFrame)(implicit spark: SparkSession): DataFrame = {

    source.createOrReplaceTempView("Source")

    val sessions = spark.sql(
      """
        |SELECT src.purchaseId, sessions.sessionId, sessions.campaignId, sessions.channelId
        |FROM
        |(SELECT userId, eventTime, purchaseId
        |FROM Source
        |WHERE eventType=='purchase') as src
        |LEFT JOIN
        |(SELECT ROW_NUMBER() OVER (partition by userId order by eventTime) as num,
        |        userId,
        |        case when eventType=='app_open' then campaignId else null end as campaignId,
        |        case when eventType=='app_open' then channelId else null end as channelId,
        |        case when eventType=='app_open' then eventId else null end as sessionId,
        |        case when eventType=='app_open' then eventTime else null end as start,
        |        case when eventType=='app_open' then lead(eventTime) over (partition by userId order by eventTime) else eventTime end as end
        |from Source
        |where eventType=='app_open' or eventType=='app_close') as sessions
        |ON sessions.num%2 == 1
        |   AND src.userId == sessions.userId
        |   AND src.eventTime BETWEEN sessions.start AND sessions.end
        |WHERE sessions.sessionId IS NOT NULL
      """.stripMargin
    )

    sessions
      .cache()
  }

  /**
    * Read data of purchases from csv file
    */
  def buildPurchasesDF(pathPurchasesFile: String)(implicit spark: SparkSession): DataFrame = {

    val purchases = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("escape", "\"")
      .load(pathPurchasesFile)

    purchases
      .cache()
  }

  /**
    * Using prepared sessions merge purchases with sessions and get projections of purchases
    */
  def buildProjectedPurchasesDF(purchases: DataFrame, sessions: DataFrame)(implicit spark: SparkSession): DataFrame = {

    sessions.createOrReplaceTempView("Sessions")
    purchases.createOrReplaceTempView("Purchases")

    val projectedPurchases = spark.sql(
      """
        |SELECT Purchases.purchaseId,
        |       Purchases.purchaseTime,
        |       Purchases.billingCost,
        |       Purchases.isConfirmed,
        |       Sessions.sessionId,
        |       Sessions.campaignId,
        |       Sessions.channelId
        |FROM Purchases
        |LEFT JOIN Sessions
        |ON Purchases.purchaseId == Sessions.purchaseId
        |WHERE Sessions.purchaseId IS NOT NULL
      """.stripMargin
    )

    projectedPurchases
      .cache()
  }

  private def fixFormat(processDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    val processedDF = processDF
      .withColumn("attributes", when(expr("attributes IS NOT NULL"), expr("substring(trim(attributes), 2, length(trim(attributes))-2)")))

    processedDF
  }

  private def addColumns(processDF: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val processedDF = processDF
      .withColumn("campaignId", when($"eventType" === EventType.APP_OPEN, get_json_object($"attributes", "$.campaign_id")))
      .withColumn("channelId", when($"eventType" === EventType.APP_OPEN, get_json_object($"attributes", "$.channel_id")))
      .withColumn("purchaseId", when($"eventType" === EventType.PURCHASE, get_json_object($"attributes", "$.purchase_id")))

    processedDF
  }

  private def buildSourceDF(source: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val fixedClicks = fixFormat(source)
    val extendedClicks = addColumns(fixedClicks)

    extendedClicks
      .cache()
  }
}
