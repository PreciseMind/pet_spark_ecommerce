package com.precisemind.pet.ecommerce.analyzer.impl

import com.precisemind.pet.ecommerce.analyzer.MarketingAnalyzer
import org.apache.spark.sql.{DataFrame, SparkSession}

object MarketingAnalyzerSQL extends MarketingAnalyzer {

  val TOP_CAMPAIGN_LOWER_BOUND = 10
  val TOP_CHANNEL_RATE = 1

  /**
    * Calculate top 10 campaigns by revenue on confirmed purchases
    */
  def top10Campaigns(projectedPurchases: DataFrame)(implicit spark: SparkSession): DataFrame = {

    projectedPurchases.createOrReplaceTempView("ProjectedPurchases")

    val top10Campaigns = spark.sql(
      s"""
        |SELECT rc.rate, rc.campaignId, rc.revenue
        |FROM (SELECT campaignId,
        |             revenue,
        |             RANK() OVER (ORDER BY revenue DESC) as rate
        |      FROM (SELECT campaignId,
        |                   SUM(billingCost) as revenue
        |            FROM ProjectedPurchases
        |            WHERE isConfirmed == true
        |            GROUP BY campaignId) as campaigns) as rc
        |WHERE rc.rate<=$TOP_CAMPAIGN_LOWER_BOUND
      """.stripMargin)

    top10Campaigns
  }

  /**
    * Calculate the most popular channel for each campaign by an unique session count in channel
    */
  def topChannelEachCampaignByUniqSession(projectedPurchases: DataFrame)(implicit spark: SparkSession): DataFrame = {

    projectedPurchases.createOrReplaceTempView("ProjectedPurchases")

    val topChannelEachCampaign = spark.sql(
      s"""
        |SELECT channelRate, channelId, campaignId
        |FROM (SELECT channelId,
        |             campaignId,
        |             ROW_NUMBER() OVER (PARTITION BY campaignId ORDER BY uniqSessionCount DESC) as channelRate
        |      FROM (SELECT campaignId,
        |                   channelId,
        |                   COUNT(sessionId) as uniqSessionCount
        |            FROM ProjectedPurchases
        |            GROUP BY campaignId, channelId) as campaignChannel) AS ratedChannel
        |WHERE channelRate==$TOP_CHANNEL_RATE
      """.stripMargin)

    topChannelEachCampaign
  }
}
