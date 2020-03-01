package com.precisemind.pet.ecommerce.model

import java.sql.Timestamp

/**
  *
  */

case class SourceClick(userId: String,
                       eventId: String,
                       eventTime: Timestamp,
                       eventType: String,
                       attributes: String)

case class FillClick(userId: String,
                     eventId: String,
                     eventTime: Timestamp,
                     eventType: String,
                     attributes: String,
                     campaignId: String,
                     channelId: String,
                     purchaseId: String)

case class FillSession(purchaseId: String, sessionId: String, campaignId: String, channelId: String)

case class FillTransaction(transactionPurchaseId: String, sessionId: String, campaignId: String, channelId: String)


case class ProjectionPurchase(purchaseId: String,
                              purchaseTime: Timestamp,
                              billingCost: Double,
                              isConfirmed: Boolean,
                              sessionId: String,
                              campaignId: String,
                              channelId: String)

case class TopCampaign(rate: Int, campaignId: String, revenue: Double)

case class TopChannel(channelRate: Int, channelId: String, campaignId: String)
