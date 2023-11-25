import sbt._
object Dependencies {
  lazy val logbackVersion = "2.20.0"
  lazy val awsClientVersion = "0.1.27"
  lazy val fs2Reactive = "co.fs2" %% "fs2-reactive-streams" % "3.7.0"
  lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j-impl" % logbackVersion
  lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % logbackVersion
  lazy val log4jTemplateJson = "org.apache.logging.log4j" % "log4j-layout-template-json" % logbackVersion
  lazy val lambdaCore = "com.amazonaws" % "aws-lambda-java-core" % "1.2.2"
  lazy val pureConfigCats = "com.github.pureconfig" %% "pureconfig-cats-effect" % "0.17.4"
  lazy val pureConfig = "com.github.pureconfig" %% "pureconfig" % "0.17.4"
  lazy val s3Client = "uk.gov.nationalarchives" %% "da-s3-client" % awsClientVersion
  lazy val dynamoClient = "uk.gov.nationalarchives" %% "da-dynamodb-client" % "0.1.17"
  lazy val scalaXml = "org.scala-lang.modules" %% "scala-xml" % "2.2.0"
  lazy val mockito = "org.mockito" %% "mockito-scala" % "1.17.12"
  lazy val wiremock = "com.github.tomakehurst" % "wiremock" % "2.27.2"
  lazy val sttp = "com.softwaremill.sttp.client3" %% "fs2" % "3.8.16"
  lazy val sttpUpickle = "com.softwaremill.sttp.client3" %% "upickle" % "3.8.16"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.15"
}
