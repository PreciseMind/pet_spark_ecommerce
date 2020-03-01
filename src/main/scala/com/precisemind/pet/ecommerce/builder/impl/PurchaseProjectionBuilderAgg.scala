package com.precisemind.pet.ecommerce.builder.impl

import com.precisemind.pet.ecommerce.builder.PurchaseProjectionBuilder
import com.precisemind.pet.ecommerce.model.{Click, EventType, Purchase, Transaction}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.{explode, expr, get_json_object, when}

import scala.collection.immutable.SortedSet

object PurchaseProjectionBuilderAgg extends PurchaseProjectionBuilder {


  /**
    * Make projections of purchases
    * The method executes a full cycle of computations -
    * reading a csv data (click events, purchases), calculating sessions of users, merging sessions with purchases that to get projections of purchases
    */
  def process(clicksPath: String, purchasesPath: String)(implicit spark: SparkSession): DataFrame = {

    var clicks = PurchaseProjectionBuilderAgg.buildSourceDS(clicksPath)

    val transactions = PurchaseProjectionBuilderAgg.buildTransactionsDS(clicks)

    val purchases = PurchaseProjectionBuilderAgg.buildPurchasesDS(purchasesPath)

    val projectedPurchases = PurchaseProjectionBuilderAgg.buildProjectedPurchaseDF(purchases, transactions)
    projectedPurchases
  }

  /**
    * Read data of click events from csv file
    */
  def buildSourceDS(pathClicksFile: String)(implicit spark: SparkSession): Dataset[Click] = {

    val clicks = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("escape", "\"")
      .load(pathClicksFile)

    buildSourceDS(clicks)
  }

  /**
    * Supportive method for building source, use for test
    */
  def buildSourceDSTest(clicks: DataFrame)(implicit spark: SparkSession): Dataset[Click] = {

    buildSourceDS(clicks)
  }


  /**
    * Using a type-safe aggregation function form a list of transactions (sessions, few transactions can be relate with the same session)
    */
  def buildTransactionsDS(processDS: Dataset[Click])(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    processDS.where($"eventType".isin(EventType.APP_OPEN,EventType.PURCHASE, EventType.APP_CLOSE))
      .groupByKey(click => click.userId)
      .agg(TransactionAggregator.toColumn.alias("transactions").as[List[Transaction]])
      .select($"value".as("userId"), $"transactions")
      .select("userId", "transactions.*")
      .select($"userId", explode($"value").as("transactionDetails"))
      .select("transactionDetails.*")
      .cache()
  }

  /**
    * Read data of purchases from csv file
    */
  def buildPurchasesDS(pathPurchasesFile: String)(implicit spark: SparkSession): Dataset[Purchase] = {

    import spark.implicits._

    val purchases = spark.read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .option("escape", "\"")
      .load(pathPurchasesFile)
      .as[Purchase]

    purchases
      .cache()
  }

  /**
    * Using prepared sessions merge purchases with sessions and get projections of purchases
    */
  def buildProjectedPurchaseDF(purchases: Dataset[Purchase], transactions: DataFrame): DataFrame = {

    val projectedPurchases = purchases.join(transactions,
      purchases.col("purchaseId") === transactions.col("transactionPurchaseId"), "left_outer")
      .where(transactions.col("transactionPurchaseId").isNotNull)
      .drop("transactionPurchaseId")

    projectedPurchases
      .cache()
  }

  private def buildSourceDS(clicks: DataFrame)(implicit spark: SparkSession): Dataset[Click] = {

    import spark.implicits._

    val fixedClicks = fixFormat(clicks)
    val extendedClicks = addColumns(fixedClicks)

    extendedClicks.as[Click]
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

  object TransactionAggregator extends Aggregator[Click, SortedSet[Click], List[Transaction]] {

    override def zero: SortedSet[Click] = SortedSet()

    override def reduce(buffer: SortedSet[Click], click: Click): SortedSet[Click] = {
      buffer + click
    }

    override def merge(firstBuffer: SortedSet[Click], secondBuffer: SortedSet[Click]): SortedSet[Click] = {
      firstBuffer.union(secondBuffer)
    }

    override def finish(reduction: SortedSet[Click]): List[Transaction] = {
      reduction.foldLeft(List[Transaction]()) { (transactions, click) => {

        val sessionId = click.eventId
        val eventType = click.eventType
        val campaignId = click.campaignId
        val channelId = click.channelId
        val purchaseId = click.purchaseId

        transactions match {
          case Nil => Transaction(purchaseId, sessionId, campaignId, channelId) :: transactions
          case transaction :: otherTransactions =>
            if (eventType == EventType.APP_OPEN) {
              Transaction(purchaseId, sessionId, campaignId, channelId) :: transactions
            } else if (eventType == EventType.PURCHASE) {
              // Update state open transaction or if open transaction was related with a purchase in this case create new transaction
              if (transaction.madePurchase()) {
                Transaction(purchaseId, transaction.sessionId, transaction.campaignId, transaction.channelId) :: transactions
              } else {
                transaction.setTransactionPurchaseId(purchaseId) :: otherTransactions
              }
            } else if (eventType == EventType.APP_CLOSE) {
              // If a transaction(session) doesn't have a purchase that to delete such session
              if (transaction.madePurchase()) transactions else otherTransactions
            } else {
              transactions
            }
        }
      }
      }
    }

    override def bufferEncoder: Encoder[SortedSet[Click]] = Encoders.kryo[SortedSet[Click]]

    override def outputEncoder: Encoder[List[Transaction]] = Encoders.product[List[Transaction]]
  }

}
