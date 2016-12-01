# fs2-sqs

[![Build Status](https://travis-ci.org/ImageIntelligence/fs2-sqs.svg?branch=master)](https://travis-ci.org/ImageIntelligence/fs2-sqs)
[![Download](https://api.bintray.com/packages/imageintelligence/maven/fs2-sqs/images/download.svg)](https://bintray.com/imageintelligence/maven/fs2-sqs/_latestVersion)

[SQS](https://aws.amazon.com/sqs/) using [FS2](https://github.com/functional-streams-for-scala/fs2).


## Overview:

- Lightweight wrapper over the raw [Java AWS SDK](https://aws.amazon.com/sdk-for-java/)

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


