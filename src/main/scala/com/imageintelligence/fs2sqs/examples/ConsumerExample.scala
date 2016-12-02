package com.imageintelligence.fs2sqs.examples

import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import fs2.Strategy
import com.amazonaws.services.sqs.model._
import java.util.concurrent.Executors

import com.amazonaws.auth.BasicAWSCredentials
import com.imageintelligence.fs2sqs.FS2SQS
import com.imageintelligence.fs2sqs.FS2SQS.MessageAction
import fs2.Pipe
import fs2.Sink
import fs2.Stream
import fs2.Task

object ConsumerExample {
  def main(args: Array[String]): Unit = {
    val tp = Executors.newFixedThreadPool(4)
    implicit val strategy = Strategy.fromExecutor(tp)

    val credentials = new BasicAWSCredentials(sys.env("II_STAGING_AWS_ACCESS_KEY"), sys.env("II_STAGING_AWS_SECRET_KEY"))
    val client = new AmazonSQSAsyncClient(credentials)

    val queueUrl = "https://sqs.ap-southeast-2.amazonaws.com/862341389713/example"

    val messageRequest = new ReceiveMessageRequest(queueUrl)
      .withMaxNumberOfMessages(1)
      .withWaitTimeSeconds(10)

    val messagesStream: Stream[Task, Message] = FS2SQS.messageStream(client, messageRequest)

    val ackSink: Sink[Task, (Message, (Message) => MessageAction)] = FS2SQS.ackSink(client)

    val workPipe: Pipe[Task, Message, (Message, (Message) => MessageAction)] = { messages =>
      messages.map { message =>
        if (message.getBody == "DOM") {
          (message, (m: Message) => Right(new DeleteMessageRequest(queueUrl, m.getReceiptHandle)))
        } else {
          (message, (m: Message) => Left(new SendMessageRequest(queueUrl, m.getBody)))
        }
      }
    }

    val effect: Stream[Task, Unit] = messagesStream
      .through(workPipe)
      .through(ackSink)

    effect.run.unsafeRun()
  }
}
