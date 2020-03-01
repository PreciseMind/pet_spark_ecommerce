package com.precisemind.pet.ecommerce

import java.sql.Timestamp

import com.precisemind.pet.ecommerce.analyzer.impl.MarketingAnalyzerDS
import com.precisemind.pet.ecommerce.builder.impl.PurchaseProjectionBuilderSQL
import com.precisemind.pet.ecommerce.model._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class MarketingAnalyzingDSTest extends AnyFunSuite {

  val AppName = "ECommerce"
  val Master = "local[3]"

  val sparkConfig: SparkConf = new SparkConf()
    .setAppName(AppName)
    .setMaster(Master)

  implicit val spark: SparkSession = SparkSession.builder()
    .config(sparkConfig)
    .getOrCreate()

  val clickTestData: Seq[SourceClick] = Seq(SourceClick("u1", "u1_e1", Timestamp.valueOf("2019-01-01 0:00:00"), "app_open", "{{\"campaign_id\": \"cmp1\",  \"channel_id\": \"Google Ads\"}}"),
    SourceClick("u1", "u1_e2", Timestamp.valueOf("2019-01-01 0:00:05"), "search_product", null),
    SourceClick("u1", "u1_e3", Timestamp.valueOf("2019-01-01 0:00:10"), "search_product", null),
    SourceClick("u1", "u1_e4", Timestamp.valueOf("2019-01-01 0:00:15"), "search_product", null),
    SourceClick("u1", "u1_e5", Timestamp.valueOf("2019-01-01 0:00:20"), "view_product_details", null),
    SourceClick("u1", "u1_e6", Timestamp.valueOf("2019-01-01 0:01:00"), "purchase", "{{\"purchase_id\": \"p1\"}}"),
    SourceClick("u1", "u1_e7", Timestamp.valueOf("2019-01-01 0:02:00"), "app_close", null),
    SourceClick("u2", "u2_e1", Timestamp.valueOf("2019-01-01 0:00:00"), "app_open", "{{\"campaign_id\": \"cmp1\",  \"channel_id\": \"Yandex Ads\"}}"),
    SourceClick("u2", "u2_e2", Timestamp.valueOf("2019-01-01 0:00:03"), "search_product", null),
    SourceClick("u2", "u2_e3", Timestamp.valueOf("2019-01-01 0:01:00"), "view_product_details", null),
    SourceClick("u2", "u2_e4", Timestamp.valueOf("2019-01-01 0:03:00"), "purchase", "{{\"purchase_id\": \"p2\"}}"),
    SourceClick("u2", "u2_e5", Timestamp.valueOf("2019-01-01 0:04:00"), "app_close", null),
    SourceClick("u2", "u2_e6", Timestamp.valueOf("2019-01-02 0:00:00"), "app_open", "{{\"campaign_id\": \"cmp2\",  \"channel_id\": \"Yandex Ads\"}}"),
    SourceClick("u2", "u2_e7", Timestamp.valueOf("2019-01-02 0:00:03"), "search_product", null),
    SourceClick("u2", "u2_e8", Timestamp.valueOf("2019-01-02 0:01:00"), "view_product_details", null),
    SourceClick("u2", "u2_e10", Timestamp.valueOf("2019-01-02 0:04:00"), "app_close", null),
    SourceClick("u3", "u3_e19", Timestamp.valueOf("2019-01-02 13:00:10"), "app_open", "{{\"campaign_id\": \"cmp2\",  \"channel_id\": \"Yandex Ads\"}}"),
    SourceClick("u3", "u3_e22", Timestamp.valueOf("2019-01-02 13:03:00"), "purchase", "{{\"purchase_id\": \"p4\"}}"),
    SourceClick("u3", "u3_e23", Timestamp.valueOf("2019-01-02 13:06:00"), "app_close", null))

  val purchaseTestData: Seq[Purchase] = Seq(
    Purchase("p1", Timestamp.valueOf("2019-01-01 0:01:05"), 100.5, true),
    Purchase("p2", Timestamp.valueOf("2019-01-01 0:03:10"), 200, true),
    Purchase("p3", Timestamp.valueOf("2019-01-01 1:12:15"), 300, false),
    Purchase("p4", Timestamp.valueOf("2019-01-02 1:12:15"), 200, true)
  )

  import spark.implicits._

  test("Test top10Campaigns") {

    val inputClicks: Dataset[SourceClick] = spark.createDataset(clickTestData)
    val inputPurchases: Dataset[Purchase] = spark.createDataset(purchaseTestData)

    val source = PurchaseProjectionBuilderSQL.buildSourceDFTest(inputClicks.toDF())
    val sessions = PurchaseProjectionBuilderSQL.buildSessionsDF(source)
    val projectedPurchases = PurchaseProjectionBuilderSQL.buildProjectedPurchasesDF(inputPurchases.toDF(), sessions)

    val testTopCampaign: TopCampaign = TopCampaign(2, "cmp2", 200.0)
    val actualRes = MarketingAnalyzerDS.top10Campaigns(projectedPurchases)
      .where($"campaignId" === testTopCampaign.campaignId and $"rate" === testTopCampaign.rate)
      .as[TopCampaign]
      .first()

    assert(actualRes == testTopCampaign)
  }

  test("Test topChannelEachCampaignByUniqSession") {

    val inputClicks: Dataset[SourceClick] = spark.createDataset(clickTestData)
    val inputPurchases: Dataset[Purchase] = spark.createDataset(purchaseTestData)
    val source = PurchaseProjectionBuilderSQL.buildSourceDFTest(inputClicks.toDF())
    val sessions = PurchaseProjectionBuilderSQL.buildSessionsDF(source)
    val projectedPurchases = PurchaseProjectionBuilderSQL.buildProjectedPurchasesDF(inputPurchases.toDF(), sessions)

    val testTopChannel: TopChannel = TopChannel(1, "Yandex Ads", "cmp2")

    val actualRes = MarketingAnalyzerDS.topChannelEachCampaignByUniqSession(projectedPurchases)
      .where($"channelId" === testTopChannel.channelId and $"campaignId" === testTopChannel.campaignId)
      .as[TopChannel]
      .first()

    assert(actualRes == testTopChannel)
  }
}
