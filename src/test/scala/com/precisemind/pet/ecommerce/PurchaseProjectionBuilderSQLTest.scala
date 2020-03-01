package com.precisemind.pet.ecommerce

import java.sql.Timestamp

import com.precisemind.pet.ecommerce.builder.impl.PurchaseProjectionBuilderSQL
import com.precisemind.pet.ecommerce.model._
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class PurchaseProjectionBuilderSQLTest extends AnyFunSuite {

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
    SourceClick("u2", "u2_e10", Timestamp.valueOf("2019-01-02 0:04:00"), "app_close", null))

  val purchaseTestData: Seq[Purchase] = Seq(
    Purchase("p1", Timestamp.valueOf("2019-01-01 0:01:05"), 100.5, true),
    Purchase("p2", Timestamp.valueOf("2019-01-01 0:03:10"), 200, true),
    Purchase("p3", Timestamp.valueOf("2019-01-01 1:12:15"), 300, false)
  )

  import spark.implicits._

  test("Test build source - parse app_open attributes") {

    val input: Dataset[SourceClick] = spark.createDataset(clickTestData)
    val testClick: FillClick = FillClick("u1", "u1_e1", Timestamp.valueOf("2019-01-01 00:00:00"), "app_open", "{\"campaign_id\": \"cmp1\",  \"channel_id\": \"Google Ads\"}", "cmp1", "Google Ads", null)
    val actualRes = PurchaseProjectionBuilderSQL.buildSourceDFTest(input.toDF())
      .where($"userId" === testClick.userId and $"eventId" === testClick.eventId)
      .as[FillClick]
      .first()

    assert(testClick == actualRes)
  }

  test("Test build source - parse purchase attributes") {

    val input: Dataset[SourceClick] = spark.createDataset(clickTestData)
    val testClick: FillClick = FillClick("u1", "u1_e6", Timestamp.valueOf("2019-01-01 0:01:00"), "purchase", "{\"purchase_id\": \"p1\"}", null, null, "p1")
    val actualRes = PurchaseProjectionBuilderSQL.buildSourceDFTest(input.toDF())
      .where($"userId" === testClick.userId and $"eventId" === testClick.eventId)
      .as[FillClick]
      .first()

    assert(testClick == actualRes)
  }

  test("Test build Sessions test") {

    val input: Dataset[SourceClick] = spark.createDataset(clickTestData)
    val testSession: FillSession = FillSession("p1", "u1_e1", "cmp1", "Google Ads")

    val source = PurchaseProjectionBuilderSQL.buildSourceDFTest(input.toDF())
    val actualRes = PurchaseProjectionBuilderSQL.buildSessionsDF(source)
      .where($"sessionId" === testSession.sessionId)
      .as[FillSession]
      .first()

    assert(testSession == actualRes)
  }

  test("Test build Projection purchase") {

    val inputClicks: Dataset[SourceClick] = spark.createDataset(clickTestData)
    val inputPurchases: Dataset[Purchase] = spark.createDataset(purchaseTestData)
    val testProjectionPurchase: ProjectionPurchase = ProjectionPurchase("p2", Timestamp.valueOf("2019-01-01 00:03:10"), 200.0, true, "u2_e1", "cmp1", "Yandex Ads")

    val source = PurchaseProjectionBuilderSQL.buildSourceDFTest(inputClicks.toDF())
    val sessions = PurchaseProjectionBuilderSQL.buildSessionsDF(source)
    val actualRes = PurchaseProjectionBuilderSQL.buildProjectedPurchasesDF(inputPurchases.toDF(), sessions)
      .where($"purchaseId" === testProjectionPurchase.purchaseId and $"sessionId" === testProjectionPurchase.sessionId)
      .as[ProjectionPurchase]
      .first()

    assert(actualRes === testProjectionPurchase)
  }
}


