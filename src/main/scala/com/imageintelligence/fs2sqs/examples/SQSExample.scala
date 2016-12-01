package com.imageintelligence.fs2sqs.examples

import java.util.concurrent.Executors
import java.util.concurrent.Future

import com.amazonaws.AmazonWebServiceRequest
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model.SendMessageBatchRequest
import com.amazonaws.services.sqs.model.SendMessageBatchResult
import com.amazonaws.services.sqs.model.SendMessageRequest
import com.amazonaws.services.sqs.model.SendMessageResult

import scala.collection.JavaConverters._
import scalaz._
import Scalaz._
import fs2._
import org.joda.time.Instant

object SQSExample {

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

  def publishPipe(client: AmazonSQSAsyncClient)(implicit s: Strategy): Pipe[Task, SendMessageRequest, SendMessageResult] = { requests =>
    requests.evalMap { request =>
      sendMessageAsync(client, request)
    }
  }

  def publishBatchPipe(client: AmazonSQSAsyncClient)(implicit s: Strategy): Pipe[Task, SendMessageBatchRequest, SendMessageBatchResult] = { requests =>
    requests.evalMap { request =>
      sendMessageBatchAsync(client, request)
    }
  }

//  def publishBatchUnchunkPipe(client: AmazonSQSAsyncClient)(implicit s: Strategy): Pipe[Task, SendMessageBatchRequest, SendMessageBatchResult] = { requests =>
//    requests.through(publishBatchPipe(client)).map(batchResult => batchResult.getSuccessful.toList.)
//  }

  private def handler[E <: AmazonWebServiceRequest, A](k: (Either[Throwable, A]) => Unit) = new AsyncHandler[E, A] {
    override def onError(exception: Exception): Unit = k(Left(exception))

    override def onSuccess(request: E, result: A): Unit = k(Right(result))
  }

//  private def convertSendMessageBatchResultToIndividual(r: SendMessageBatchResult): List[SendMessageResult] = {
//    r.get
//  }

  def loggingSink[A]: Sink[Task, A] = { s =>
    s.map { i =>
      println(i)
    }
  }


  def main(args: Array[String]): Unit = {

    implicit val strategy = Strategy.fromExecutor(Executors.newFixedThreadPool(4))

    val credentials = new BasicAWSCredentials(sys.env("II_STAGING_AWS_ACCESS_KEY"), sys.env("II_STAGING_AWS_SECRET_KEY"))
    val sqs = new AmazonSQSAsyncClient(credentials)

    val messages = Stream.emits[Task, SendMessageRequest]((0 until 10000).map(x => new SendMessageRequest("https://sqs.ap-southeast-2.amazonaws.com/862341389713/example", "DOM")))

    val publisher = publishPipe(sqs)

    val eff = messages.through(publisher).to(loggingSink)

    val start = Instant.now

    eff.run.unsafeRun()

    println("Took: " + (Instant.now.getMillis - start.getMillis))

  }

}
