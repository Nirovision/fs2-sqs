package com.imageintelligence.fs2sqs

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model._
import fs2.Strategy
import fs2.Task

object AsyncSQS {

  def sendMessageAsync(client: AmazonSQSAsyncClient, request: SendMessageRequest)(implicit s: Strategy): Task[SendMessageResult] = {
    Task.async[SendMessageResult] { k =>
      client.sendMessageAsync(request, handler[SendMessageRequest, SendMessageResult](k))
    }
  }

  def sendMessageBatchAsync(client: AmazonSQSAsyncClient, request: SendMessageBatchRequest)(implicit s: Strategy): Task[SendMessageBatchResult] = {
    Task.async[SendMessageBatchResult] { k =>
      client.sendMessageBatchAsync(request, handler[SendMessageBatchRequest, SendMessageBatchResult](k))
    }
  }

  def getMessagesAsync(client: AmazonSQSAsyncClient, request: ReceiveMessageRequest)(implicit s: Strategy): Task[ReceiveMessageResult] = {
    Task.async[ReceiveMessageResult] { k =>
      client.receiveMessageAsync(request, handler[ReceiveMessageRequest, ReceiveMessageResult](k))
    }
  }

  def deleteMessageAsync(client: AmazonSQSAsyncClient, request: DeleteMessageRequest)(implicit s: Strategy): Task[DeleteMessageResult] = {
    Task.async[DeleteMessageResult] { k =>
      client.deleteMessageAsync(request, handler[DeleteMessageRequest, DeleteMessageResult](k))
    }
  }

  def deleteMessageBatchAsync(client: AmazonSQSAsyncClient, request: DeleteMessageBatchRequest)(implicit s: Strategy): Task[DeleteMessageBatchResult] = {
    Task.async[DeleteMessageBatchResult] { k =>
      client.deleteMessageBatchAsync(request)
    }
  }

  private def handler[E <: AmazonWebServiceRequest, A](k: (Either[Throwable, A]) => Unit) = new AsyncHandler[E, A] {
    override def onError(exception: Exception): Unit = k(Left(exception))

    override def onSuccess(request: E, result: A): Unit = k(Right(result))
  }

}
