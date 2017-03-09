package com.imageintelligence.fs2sqs

import java.util.UUID

import scala.collection.JavaConverters._
import com.amazonaws.services.sqs._
import com.amazonaws.services.sqs.model._
import fs2._

sealed trait FS2SQSRequest
case class FS2DeleteMsgRequest(req: DeleteMessageRequest) extends FS2SQSRequest
case class FS2SendMsgRequest(req: SendMessageRequest) extends FS2SQSRequest
case class FS2ChangeMsgVisibilityRequest(req: ChangeMessageVisibilityRequest) extends FS2SQSRequest

object FS2SQS {

  def publishPipe(client: AmazonSQSAsyncClient)(implicit s: Strategy): Pipe[Task, SendMessageRequest, SendMessageResult] = { requests =>
    requests.evalMap { request =>
      AsyncSQS.sendMessageAsync(client, request)
    }
  }

  def publishBatchPipe(client: AmazonSQSAsyncClient)(implicit s: Strategy): Pipe[Task, SendMessageBatchRequest, SendMessageBatchResult] = { requests =>
    requests.evalMap { request =>
      AsyncSQS.sendMessageBatchAsync(client, request)
    }
  }

  def messageStream(client: AmazonSQSAsyncClient, request: ReceiveMessageRequest)(implicit s: Strategy): Stream[Task, Message] = {
    Stream.repeatEval(AsyncSQS.getMessagesAsync(client, request))
      .flatMap(result => Stream.emits(result.getMessages.asScala))
  }

  def ackSink(client: AmazonSQSAsyncClient)(implicit s: Strategy): Sink[Task, (Message, (Message => FS2SQSRequest))] = { mes =>
    mes.map {
      case (message, action) => action(message)
    }.evalMap {
      case FS2DeleteMsgRequest(req) => AsyncSQS.deleteMessageAsync(client, req)
      case FS2SendMsgRequest(req) => AsyncSQS.sendMessageAsync(client, req)
      case FS2ChangeMsgVisibilityRequest(req) => AsyncSQS.changeMessageVisibilityAsync(client, req)
    }.map(_ => ())
  }

  def batchAckSink(client: AmazonSQSAsyncClient, maxBatchSize: Int)(implicit s: Strategy): Sink[Task, (Message, (Message => FS2SQSRequest))] = { mes =>
    val actions = mes.map {
      case (message, action) => action(message)
    }

    val deleteBatch = actions.collect {
      case FS2DeleteMsgRequest(d) => d
    }.chunkN(maxBatchSize, allowFewer = true)
      .map(x => x.toList.map(_.head))
      .flatMap { items =>
        val requests = items
          .groupBy(x => x.getQueueUrl)
          .map { case (queueUrl, reqs) =>
            val entries = reqs.map(p => new DeleteMessageBatchRequestEntry(UUID.randomUUID().toString, p.getReceiptHandle)).asJava
            new DeleteMessageBatchRequest(queueUrl, entries)
          }
        Stream.emits(requests.toList)
      }.evalMap { deleteMessageRequest =>
        AsyncSQS.deleteMessageBatchAsync(client, deleteMessageRequest)
      }

    val sendsBatch = actions.collect {
      case FS2SendMsgRequest(d) => d
    }.chunkN(maxBatchSize, allowFewer = true)
      .map(x => x.toList.map(_.head))
      .flatMap { items =>
        val requests = items
          .groupBy(x => x.getQueueUrl)
          .map { case (queueUrl, reqs) =>
            val entries = reqs.map(p => new SendMessageBatchRequestEntry(UUID.randomUUID().toString, p.getMessageBody)).asJava
            new SendMessageBatchRequest(queueUrl, entries)
          }
        Stream.emits(requests.toList)
      }.evalMap { sendMessageRequest =>
        AsyncSQS.sendMessageBatchAsync(client, sendMessageRequest)
      }

    val changeVisibilityBatch = actions.collect {
      case FS2ChangeMsgVisibilityRequest(d) => d
    }.chunkN(maxBatchSize, allowFewer = true)
      .map(x => x.toList.map(_.head))
      .flatMap { items =>
        val requests = items
          .groupBy(x => x.getQueueUrl)
          .map { case (queueUrl, reqs) =>
            val entries = reqs.map(p => new ChangeMessageVisibilityBatchRequestEntry(UUID.randomUUID().toString, p.getReceiptHandle)).asJava
            new ChangeMessageVisibilityBatchRequest(queueUrl, entries)
          }
        Stream.emits(requests.toList)
      }.evalMap { changeMessageVisibilityRequest =>
      AsyncSQS.changeMessageVisibilityBatchAsync(client, changeMessageVisibilityRequest)
    }

    deleteBatch.merge(sendsBatch).merge(changeVisibilityBatch).map(_ => ())
  }
}
