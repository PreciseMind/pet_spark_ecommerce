package com.precisemind.pet.ecommerce.builder

import org.apache.spark.sql.{DataFrame, SparkSession}

trait PurchaseProjectionBuilder {
  def process(clicksPath: String, purchasesPath: String)(implicit spark: SparkSession): DataFrame
}
