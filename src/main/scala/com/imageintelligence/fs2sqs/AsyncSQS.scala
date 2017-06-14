package com.imageintelligence.fs2sqs

import cats.effect.Effect
import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model._

object AsyncSQS {

  def sendMessageAsync[F[_]: Effect](client: AmazonSQSAsyncClient, request: SendMessageRequest): F[SendMessageResult] = {
    asyncF[F, SendMessageResult] { k =>
      client.sendMessageAsync(request, handler[SendMessageRequest, SendMessageResult](k))
    }
  }

  def sendMessageBatchAsync[F[_]: Effect](client: AmazonSQSAsyncClient, request: SendMessageBatchRequest): F[SendMessageBatchResult] = {
    asyncF[F, SendMessageBatchResult] { k =>
      client.sendMessageBatchAsync(request, handler[SendMessageBatchRequest, SendMessageBatchResult](k))
    }
  }

  def getMessagesAsync[F[_]: Effect](client: AmazonSQSAsyncClient, request: ReceiveMessageRequest): F[ReceiveMessageResult] = {
    asyncF[F, ReceiveMessageResult] { k =>
      client.receiveMessageAsync(request, handler[ReceiveMessageRequest, ReceiveMessageResult](k))
    }
  }

  def deleteMessageAsync[F[_]: Effect](client: AmazonSQSAsyncClient, request: DeleteMessageRequest): F[DeleteMessageResult] = {
    asyncF[F, DeleteMessageResult] { k =>
      client.deleteMessageAsync(request, handler[DeleteMessageRequest, DeleteMessageResult](k))
    }
  }

  def deleteMessageBatchAsync[F[_]: Effect](client: AmazonSQSAsyncClient, request: DeleteMessageBatchRequest): F[DeleteMessageBatchResult] = {
    asyncF[F, DeleteMessageBatchResult] { k =>
      client.deleteMessageBatchAsync(request, handler[DeleteMessageBatchRequest, DeleteMessageBatchResult](k))
    }
  }

  def changeMessageVisibilityAsync[F[_]: Effect](client: AmazonSQSAsyncClient, request: ChangeMessageVisibilityRequest): F[ChangeMessageVisibilityResult] = {
    asyncF[F, ChangeMessageVisibilityResult] { k =>
      client.changeMessageVisibilityAsync(request, handler[ChangeMessageVisibilityRequest, ChangeMessageVisibilityResult](k))
    }
  }

  def changeMessageVisibilityBatchAsync[F[_]: Effect](client: AmazonSQSAsyncClient, request: ChangeMessageVisibilityBatchRequest): F[ChangeMessageVisibilityBatchResult] = {
    asyncF[F, ChangeMessageVisibilityBatchResult] { k =>
      client.changeMessageVisibilityBatchAsync(request, handler[ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchResult](k))
    }
  }

  def asyncF[F[_], A](k: (Either[Throwable, A] => Unit) => Unit)(implicit f: Effect[F]): F[A] = {
    f.async(k)
  }

  private def handler[E <: AmazonWebServiceRequest, A](k: (Either[Throwable, A]) => Unit) = new AsyncHandler[E, A] {
    override def onError(exception: Exception): Unit = k(Left(exception))

    override def onSuccess(request: E, result: A): Unit = k(Right(result))
  }

}


