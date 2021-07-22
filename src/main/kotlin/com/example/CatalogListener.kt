package com.example

import io.micronaut.gcp.pubsub.annotation.PubSubListener
import io.micronaut.gcp.pubsub.annotation.Subscription
import io.micronaut.gcp.pubsub.exception.PubSubMessageReceiverException
import io.micronaut.gcp.pubsub.exception.PubSubMessageReceiverExceptionHandler
import io.micronaut.messaging.Acknowledgement
import org.slf4j.Logger
import org.slf4j.LoggerFactory


@PubSubListener
open class CatalogPipeline: PubSubMessageReceiverExceptionHandler {
    /** Private log pipe. */
    val logging: Logger = LoggerFactory.getLogger(CatalogPipeline::class.java)

    init {
        logging.info("Initializing catalog pipeline...")
    }

    @Subscription(value = "v1.catalog.commit")
    fun onMessage(data: ByteArray, acknowledgement: Acknowledgement) {
        logging.info("!! Message received via Pub/Sub trigger channel. !!")
    }

    override fun handle(exception: PubSubMessageReceiverException) {
        val listener = exception.listener
        val state = exception.state
        val originalMessage = state.pubsubMessage
        val contentType = state.contentType

//        state.ackReplyConsumer.ack()
        logging.error("!!! Error")
    }
}

