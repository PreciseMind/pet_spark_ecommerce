package com.precisemind.pet.ecommerce.analyzer.impl

import com.precisemind.pet.ecommerce.analyzer.MarketingAnalyzer
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, rank, row_number, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MarketingAnalyzerDS extends MarketingAnalyzer {

  val TOP_CAMPAIGN_LOWER_BOUND = 10
  val TOP_CHANNEL_RATE = 1

  /**
    * Calculate top 10 campaigns by revenue on confirmed purchases
    */
  def top10Campaigns(projectedPurchases: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val top10Campaigns = projectedPurchases
      .where($"isConfirmed" === true)
      .groupBy($"campaignId")
      .agg(sum($"billingCost").alias("revenue"))
      .select($"campaignId",
        $"revenue",
        rank().over(Window.orderBy($"revenue".desc)).alias("rate"))
      .where($"rate" <= TOP_CAMPAIGN_LOWER_BOUND)

    top10Campaigns
  }

  /**
    * Calculate the most popular channel for each campaign by an unique session count in channel
    */
  def topChannelEachCampaignByUniqSession(projectedPurchases: DataFrame)(implicit spark: SparkSession): DataFrame = {

    import spark.implicits._

    val topChannelEachCampaign = projectedPurchases
      .groupBy($"campaignId", $"channelId")
      .agg(count($"sessionId").alias("uniqSessionCount"))
      .select($"channelId",
        $"campaignId",
        row_number().over(Window.partitionBy($"campaignId").orderBy($"uniqSessionCount".desc)).alias("channelRate"))
      .where($"channelRate" === TOP_CHANNEL_RATE)

    topChannelEachCampaign
  }
}
