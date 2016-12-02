# fs2-sqs

[![Build Status](https://travis-ci.org/ImageIntelligence/fs2-sqs.svg?branch=master)](https://travis-ci.org/ImageIntelligence/fs2-sqs)
[![Download](https://api.bintray.com/packages/imageintelligence/maven/fs2-sqs/images/download.svg)](https://bintray.com/imageintelligence/maven/fs2-sqs/_latestVersion)

[SQS](https://aws.amazon.com/sqs/) using [FS2](https://github.com/functional-streams-for-scala/fs2).


## Overview:

- Lightweight wrapper over the raw [Java AWS SDK](https://aws.amazon.com/sdk-for-java/). Doesn't wrap the types, so you
gain the full power of the AWS SDK and aren't forced around a leaky abstraction.
- Unopinionated primitive building blocks.

## Quick examples

Included are explicit types for the sake of clarity 

### Publishing messages

```scala
val tp = Executors.newFixedThreadPool(4)
implicit val strategy = Strategy.fromExecutor(tp)
val credentials = new BasicAWSCredentials(sys.env("II_STAGING_AWS_ACCESS_KEY"), sys.env("II_STAGING_AWS_SECRET_KEY"))
val client = new AmazonSQSAsyncClient(credentials)
val queueUrl = "https://sqs.ap-southeast-2.amazonaws.com/862341389713/example"

// Construct an infinite Stream SendMessageRequest's, with the same body "123"
val messageRequestsStream: Stream[Task, SendMessageRequest] =
  Stream.constant(new SendMessageRequest(queueUrl, "123")).repeat

// Construct a Publish pipe that can turn SendMessageRequest's into SendMessageResult's
val publishPipe: Pipe[Task, SendMessageRequest, SendMessageResult] = FS2SQS.publishPipe(client)

def loggingSink[A]: Sink[Task, A] = { s =>
  s.map { i =>
    println(i)
  }
}

// Compose our stream and pipe.
val effect = messageRequestsStream
  .through(publishPipe)
  .to(loggingSink)
  .onError(e => Stream.emit(println("Error: " + e.getMessage)))

// Lift our effect into a Task, and run it.
effect.run.unsafeRun()
```

### Consuming messages

```scala
val tp = Executors.newFixedThreadPool(4)
implicit val strategy = Strategy.fromExecutor(tp)

val credentials = new BasicAWSCredentials(sys.env("II_STAGING_AWS_ACCESS_KEY"), sys.env("II_STAGING_AWS_SECRET_KEY"))
val client = new AmazonSQSAsyncClient(credentials)

val queueUrl = "https://sqs.ap-southeast-2.amazonaws.com/862341389713/example"

val messageRequest = new ReceiveMessageRequest(queueUrl)
  .withMaxNumberOfMessages(1)
  .withWaitTimeSeconds(10)

val messagesStream: Stream[Task, Message] = FS2SQS.messageStream(client, messageRequest)

val ackSink: Sink[Task, (Message, (Message) => MessageAction)] = FS2SQS.ackSinkAlt(client)

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
```


## Pipes, Streams and Sinks

The FS2 primitives are provided in FS2SQS.scala. You can use these as building blocks around SQS. 

### publishPipe `Pipe[Task, SendMessageRequest, SendMessageResult]`

Publishes messages to SQS.

### messageStream `Stream[Task, Message]`

An infinite stream of SQS messages.

### ackSink `Sink[Task, (Message, (Message => MessageAction))]`

A sink that accepts functions from `Message => MessageAction`. `MessageAction` is a type alias for 
`Either[SendMessageRequest, DeleteMessageRequest]`. In other words, this sink accepts functions from `Message` to 
either a `SendMessageRequest` (for requeuing), or `DeleteMessageRequest` (for successful acknowledgement).

## Installation

### As a library:

Just add this to your build.sbt

```
"com.imageintelligence" %% "fs2-sqs" % "1.0.0"
```

### As a project to work on

Clone the repository:

```
git clone https://github.com/ImageIntelligence/fs2-sqs.git
```

Compile

```
sbt compile
```

Test

```
sbt test
```

## Examples:

Please see the [examples](https://github.com/imageintelligence/fs2-sqs/tree/master/src/main/scala/com/imageintelligence/fs2-sqs/examples) directory.


