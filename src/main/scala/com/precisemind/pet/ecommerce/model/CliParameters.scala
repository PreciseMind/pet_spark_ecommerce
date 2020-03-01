package com.precisemind.pet.ecommerce.model

import org.rogach.scallop.{ScallopConf, ScallopOption}

class CliParameters(arguments: Seq[String]) extends ScallopConf(arguments) {
  val appClickstreamPath: ScallopOption[String] = opt[String](required = true, name = "clickstream-path")
  val purchasesPath: ScallopOption[String] = opt[String](required = true, name = "purchases-path")
  val outputTopCampaignPath: ScallopOption[String] = opt[String](required = true, name = "output-top-campaign-path")
  val outputTopChannelPath: ScallopOption[String] = opt[String](required = true, name = "output-top-channel-path")
  val outputSaveMode: ScallopOption[String] = opt[String](name = "output-save-mode")
  verify()
}
