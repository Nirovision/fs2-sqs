package com.imageintelligence.fs2sqs

import java.util.UUID

import scala.collection.JavaConverters._
import com.amazonaws.services.sqs._
import com.amazonaws.services.sqs.model._
import fs2._

import scala.collection.immutable.Seq

object FS2SQS {

  type MessageAction = Either[SendMessageRequest, DeleteMessageRequest]

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

  def ackSink(client: AmazonSQSAsyncClient)(implicit s: Strategy): Sink[Task, (Message, (Message => MessageAction))] = { mes =>
    mes.map {
      case (message, action) => action(message)
    }.evalMap {
      case Right(deleteMessageRequest) => AsyncSQS.deleteMessageAsync(client, deleteMessageRequest)
      case Left(sendMessageRequest)   => AsyncSQS.sendMessageAsync(client, sendMessageRequest)
    }.map(_ => ())
  }

  def batchAckSink(client: AmazonSQSAsyncClient, maxBatchSize: Int)(implicit s: Strategy): Sink[Task, (Message, (Message => MessageAction))] = { mes =>
    val actions = mes.map {
      case (message, action) => action(message)
    }

    val deleteBatch = actions.collect {
      case Right(d) => d
    }.chunkN(maxBatchSize, true)
      .map(x => x.toList.map(_.head))
      .flatMap { items =>
        val requests = items
          .groupBy(x => x.getQueueUrl)
          .map { case (queueUrl, requests) =>
            val entries = requests.map(p => new DeleteMessageBatchRequestEntry(UUID.randomUUID().toString, p.getReceiptHandle)).asJava
            new DeleteMessageBatchRequest(queueUrl, entries)
          }
        Stream.emits(requests.toList)
      }.evalMap { deleteMessageRequest =>
        AsyncSQS.deleteMessageBatchAsync(client, deleteMessageRequest)
      }

    val sendsBatch = actions.collect {
      case Left(d) => d
    }.chunkN(maxBatchSize, true)
      .map(x => x.toList.map(_.head))
      .flatMap { items =>
        val requests = items
          .groupBy(x => x.getQueueUrl)
          .map { case (queueUrl, requests) =>
            val entries = requests.map(p => new SendMessageBatchRequestEntry(UUID.randomUUID().toString, p.getMessageBody)).asJava
            new SendMessageBatchRequest(queueUrl, entries)
          }
        Stream.emits(requests.toList)
      }.evalMap { sendMessageRequest =>
        AsyncSQS.sendMessageBatchAsync(client, sendMessageRequest)
      }


    deleteBatch.merge(sendsBatch).map(_ => ())
  }
}
