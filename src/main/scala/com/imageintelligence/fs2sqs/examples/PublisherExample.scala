package com.imageintelligence.fs2sqs.examples

import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model._
import fs2.Stream
import java.util.concurrent.Executors

import cats.effect.IO
import com.amazonaws.auth.BasicAWSCredentials
import com.imageintelligence.fs2sqs.FS2SQS
import fs2._

import scala.concurrent.ExecutionContext

object PublisherExample {

  def loggingSink[F[_], A]: Sink[F, A] = { s =>
    s.map { i =>
      println(i)
    }
  }

  def main(args: Array[String]): Unit = {
    val tp = Executors.newFixedThreadPool(4)
    implicit val ec = ExecutionContext.fromExecutorService(tp)
    val credentials = new BasicAWSCredentials(sys.env("AWS_ACCESS_KEY"), sys.env("AWS_SECRET_KEY"))
    val client = new AmazonSQSAsyncClient(credentials)
    val queueUrl = "https://sqs.ap-southeast-2.amazonaws.com/1234/example"

    // Construct an infinite Stream SendMessageRequest's, with the same body "123"
    val messageRequestsStream: Stream[IO, SendMessageRequest] =
      Stream.constant(new SendMessageRequest(queueUrl, "123")).repeat

    // Construct a Publish pipe that can turn SendMessageRequest's into SendMessageResult's
    val publishPipe: Pipe[IO, SendMessageRequest, SendMessageResult] = FS2SQS.publishPipe(client)

    // Compose our stream and pipe.
    val effect = messageRequestsStream
      .through(publishPipe)
      .to(loggingSink)
      .handleErrorWith(e => Stream.emit(println("Error: " + e.getMessage)))

    // Lift our effect into a Task, and run it.
    effect.run.unsafeRunSync()
  }
}
