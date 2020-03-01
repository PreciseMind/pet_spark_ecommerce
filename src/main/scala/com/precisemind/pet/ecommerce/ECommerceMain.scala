package com.precisemind.pet.ecommerce

import com.precisemind.pet.ecommerce.analyzer.impl.MarketingAnalyzerDS
import com.precisemind.pet.ecommerce.builder.impl.PurchaseProjectionBuilderSQL
import com.precisemind.pet.ecommerce.model.CliParameters
import org.apache.spark.sql._

object ECommerceMain {

  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    val cliConfigs = new CliParameters(args)

    val projectedPurchases = PurchaseProjectionBuilderSQL.process(cliConfigs.appClickstreamPath(), cliConfigs.purchasesPath())
    val top10Campaigns = MarketingAnalyzerDS.top10Campaigns(projectedPurchases)
    saveToCSVFile(cliConfigs.outputTopCampaignPath(), top10Campaigns, cliConfigs.outputSaveMode.getOrElse("overwrite"))

    val topChannelEachCampaignByUniqSession = MarketingAnalyzerDS.topChannelEachCampaignByUniqSession(projectedPurchases)
    saveToCSVFile(cliConfigs.outputTopChannelPath(), topChannelEachCampaignByUniqSession, cliConfigs.outputSaveMode.getOrElse("overwrite"))
  }

  def saveToCSVFile(csvFilePath: String, outputData: DataFrame, mode: String = "overwrite"): Unit = {

    outputData
      .write
      .format("csv")
      .mode(mode)
      .option("ignoreLeadingWhiteSpace", "false")
      .option("ignoreTrailingWhiteSpace", "false")
      .option("header", "true")
      .save(csvFilePath)
  }
}
