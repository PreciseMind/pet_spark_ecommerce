package com.precisemind.pet.ecommerce.analyzer

import org.apache.spark.sql.{DataFrame, SparkSession}

trait MarketingAnalyzer {
  def top10Campaigns(projectedPurchases: DataFrame)(implicit spark: SparkSession): DataFrame

  def topChannelEachCampaignByUniqSession(projectedPurchases: DataFrame)(implicit spark: SparkSession): DataFrame
}
