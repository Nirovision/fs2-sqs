package com.imageintelligence.fs2sqs.examples

import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.model._
import java.util.concurrent.Executors

import cats.effect.IO
import com.amazonaws.auth.BasicAWSCredentials
import com.imageintelligence.fs2sqs.{FS2SQS, FS2SendMsgRequest, FS2SQSRequest, FS2DeleteMsgRequest}
import fs2._

import scala.concurrent.ExecutionContext

object ConsumerExample {
  def main(args: Array[String]): Unit = {
    val tp = Executors.newFixedThreadPool(4)
    implicit val ec = ExecutionContext.fromExecutorService(tp)
    val credentials = new BasicAWSCredentials(sys.env("AWS_ACCESS_KEY"), sys.env("AWS_SECRET_KEY"))
    val client = new AmazonSQSAsyncClient(credentials)
    val queueUrl = "https://sqs.ap-southeast-2.amazonaws.com/862341389713/example"

    // Construct a request to get messages from SQS
    val messageRequest = new ReceiveMessageRequest(queueUrl)
      .withMaxNumberOfMessages(10)
      .withWaitTimeSeconds(10)

    // Construct an infinite stream of Messages from SQS
    val messagesStream: Stream[IO, Message] = FS2SQS.messageStream[IO](client, messageRequest)

    // A sink that can acknowledge Messages using a MessageAction
    val ackSink: Sink[IO, (Message, (Message) => FS2SQSRequest)] = FS2SQS.batchAckSink[IO](client, 10)

    // A pipe that either deletes or requeues the message
    val workPipe: Pipe[IO, Message, (Message, (Message) => FS2SQSRequest)] = { messages =>
      messages.map { message =>
        if (message.getBody == "DOM") {
          (message, (m: Message) => FS2DeleteMsgRequest(new DeleteMessageRequest(queueUrl, m.getReceiptHandle)))
        } else {
          (message, (m: Message) => FS2SendMsgRequest(new SendMessageRequest(queueUrl, m.getBody)))
        }
      }
    }

    // Compose our stream, work pipe and ack sink
    val effect: Stream[IO, Unit] = messagesStream
      .through(workPipe)
      .through(ackSink)

    // Lift our effect into a Task, and run it.
    effect.run.unsafeRunSync()
    tp.shutdown()
  }
}
