package com.imageintelligence.fs2sqs

import java.util.UUID

import cats.effect.Effect
import cats.implicits._

import scala.collection.JavaConverters._
import com.amazonaws.services.sqs._
import com.amazonaws.services.sqs.model._
import fs2._

import scala.concurrent.ExecutionContext

sealed trait FS2SQSRequest
case class FS2DeleteMsgRequest(req: DeleteMessageRequest) extends FS2SQSRequest
case class FS2SendMsgRequest(req: SendMessageRequest) extends FS2SQSRequest
case class FS2ChangeMsgVisibilityRequest(req: ChangeMessageVisibilityRequest) extends FS2SQSRequest

object FS2SQS {

  def publishPipe[F[_]: Effect](client: AmazonSQSAsyncClient)(implicit ec: ExecutionContext): Pipe[F, SendMessageRequest, SendMessageResult] = { requests =>
    requests.evalMap { request =>
      AsyncSQS.sendMessageAsync(client, request)
    }
  }

  def publishBatchPipe[F[_]: Effect](client: AmazonSQSAsyncClient)(implicit ec: ExecutionContext): Pipe[F, SendMessageBatchRequest, SendMessageBatchResult] = { requests =>
    requests.evalMap { request =>
      AsyncSQS.sendMessageBatchAsync(client, request)
    }
  }

  def messageStream[F[_]: Effect](client: AmazonSQSAsyncClient, request: ReceiveMessageRequest)(implicit ec: ExecutionContext): Stream[F, Message] = {
    Stream.repeatEval(AsyncSQS.getMessagesAsync(client, request))
      .flatMap(result => Stream.emits(result.getMessages.asScala))
  }

  def ackSink[F[_]: Effect](client: AmazonSQSAsyncClient)(implicit ec: ExecutionContext): Sink[F, (Message, (Message => FS2SQSRequest))] = { mes =>
    mes.map {
      case (message, action) => action(message)
    }.evalMap {
      case FS2DeleteMsgRequest(req) => AsyncSQS.deleteMessageAsync[F](client, req).void
      case FS2SendMsgRequest(req) => AsyncSQS.sendMessageAsync[F](client, req).void
      case FS2ChangeMsgVisibilityRequest(req) => AsyncSQS.changeMessageVisibilityAsync[F](client, req).void
    }
  }

  def batchAckSink[F[_]: Effect](client: AmazonSQSAsyncClient, maxBatchSize: Int)(implicit ec: ExecutionContext): Sink[F, (Message, (Message => FS2SQSRequest))] = { mes =>
    val actions = mes.map {
      case (message, action) => action(message)
    }

    val deleteBatch = actions.collect {
      case FS2DeleteMsgRequest(d) => d
    }.segmentN(maxBatchSize, allowFewer = true)
      .map(x => x.toList)
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
    }.segmentN(maxBatchSize, allowFewer = true)
      .map(x => x.toList)
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
    }.segmentN(maxBatchSize, allowFewer = true)
      .map(x => x.toList)
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
