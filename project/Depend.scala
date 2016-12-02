import sbt._

object Depend {
  lazy val scalazVersion = "7.1.7"

  lazy val fs2 = Seq(
    "co.fs2" %% "fs2-core" % "0.9.2"
  )

  lazy val awsSqs = Seq(
    "com.amazonaws" % "aws-java-sdk-sqs" % "1.11.33"
  )

  lazy val scalaTestCheck = Seq(
    "org.scalatest"   %% "scalatest"                 % "2.2.4",
    "org.scalacheck"  %% "scalacheck"                % "1.12.2"
  ).map(_.withSources).map(_ % "test")

  lazy val depResolvers = Seq(
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
    Resolver.sonatypeRepo("releases")
  )

  lazy val dependencies = 
    fs2 ++
    awsSqs ++
    scalaTestCheck
}
