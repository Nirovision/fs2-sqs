package com.imageintelligence.fs2sqs

import scala.collection.JavaConverters._
import com.amazonaws.services.sqs._
import com.amazonaws.services.sqs.model._
import fs2._

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
}
