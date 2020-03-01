package com.precisemind.pet.ecommerce.model

import java.sql.Timestamp

/**
  * Classes describe
  */

object EventType {
  val APP_OPEN = "app_open"
  val SEARCH_PRODUCT = "search_product"
  val VIEW_PRODUCT_DETAILS = "view_product_details"
  val PURCHASE = "purchase"
  val APP_CLOSE = "app_close"
}

case class Click(userId: String,
                 eventId: String,
                 eventTime: Timestamp,
                 eventType: String,
                 attributes: String,
                 campaignId: String,
                 channelId: String,
                 purchaseId: String) extends Ordered[Click] {
  override def compare(that: Click): Int = {
    if (this.eventTime == that.eventTime)
      0
    else if (this.eventTime.after(that.eventTime))
      1
    else
      -1
  }
}

// Use mutable fields for optimisation GS because we can collect many deleted objects in aggregation function
case class Transaction(var transactionPurchaseId: String, var sessionId: String, var campaignId: String, var channelId: String) {
  def madePurchase(): Boolean = {
    this.transactionPurchaseId != null
  }

  def setTransactionPurchaseId(transactionPurchaseId: String): Transaction = {
    this.transactionPurchaseId = transactionPurchaseId
    this
  }
}

case class Purchase(purchaseId: String,
                    purchaseTime: Timestamp,
                    billingCost: Double,
                    isConfirmed: Boolean)
