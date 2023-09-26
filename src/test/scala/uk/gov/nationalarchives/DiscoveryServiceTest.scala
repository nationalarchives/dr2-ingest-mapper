package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.IO.asyncForIO
import cats.effect.unsafe.implicits.global
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.UriContext
import sttp.client3.impl.cats.CatsMonadError
import sttp.client3.testing.SttpBackendStub
import ujson.Obj
import uk.gov.nationalarchives.Lambda.Input
import java.util.UUID

class DiscoveryServiceTest extends AnyFlatSpec {

  val uuids: List[String] = List(
    "c7e6b27f-5778-4da8-9b83-1b64bbccbd03",
    "61ac0166-ccdf-48c4-800f-29e5fba2efda",
    "457cc27d-5b74-4e81-80e3-d808e0b3e425",
    "2b9e5c87-2342-4006-b1aa-5308a5ce2544",
    "a504d58d-2f7d-4f29-b5b8-173b558970db",
    "41c5604d-70b3-44d1-aa1f-d9ffe18b33cb"
  )

  val uuidsIterator: Iterator[String] = uuids.iterator
  val uuidIterator: () => UUID = () => UUID.fromString(uuidsIterator.next())

  val baseUrl = "http://localhost"
  val bodyMap: Map[String, String] = List("T", "T TEST").map { col =>
    val description = <scopecontent>
        <head>Head</head>
        <p><list>
          <item>TestDescription {col} 1</item>
          <item>TestDescription {col} 2</item></list>
        </p>
      </scopecontent>.toString().replaceAll("\n", "")

    val body =
      s"""{
         |  "assets": [
         |    {
         |      "citableReference": "$col",
         |      "scopeContent": {
         |        "description": "$description"
         |      },
         |      "title": "<unittitle>Test Title $col</unittitle>"
         |    }
         |  ]
         |}
         |""".stripMargin
    col -> body
  }.toMap

  private def checkDynamoTable(table: Obj, collection: String, expectedId: String, parentPath: Option[String]): Assertion = {
    table("id").str should equal(expectedId)
    table("name").str should equal(collection)
    table("title").str should equal(s"Test Title $collection")
    table("batchId").str should equal("testBatch")
    table("type").str should equal("ArchiveFolder")
    !table.value.contains("fileSize") should be(true)
    table.value.get("parentPath").map(_.str) should equal(parentPath)
    table("description").str should equal(s"TestDescription $collection 1          \nTestDescription $collection 2")
  }

  "getDepartmentAndSeriesRows" should "return the correct values for series and department" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T"))
      .thenRespond(bodyMap("T"))
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T TEST"))
      .thenRespond(bodyMap("T TEST"))

    val result = new DiscoveryService(baseUrl, backend, uuidIterator)
      .getDepartmentAndSeriesRows(Input("testBatch", "", "", Option("T"), Option("T TEST")))
      .unsafeRunSync()

    val department = result.department
    val series = result.series.head

    checkDynamoTable(department, "T", uuids.head, None)
    checkDynamoTable(series, "T TEST", uuids.tail.head, Option(uuids.head))
  }

  "getDepartmentAndSeriesRows" should "return an error if the department reference doesn't match the input" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/A"))
      .thenRespond(bodyMap("T"))
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T TEST"))
      .thenRespond(bodyMap("T TEST"))

    val ex = intercept[Exception] {
      new DiscoveryService(baseUrl, backend, uuidIterator)
        .getDepartmentAndSeriesRows(Input("testBatch", "", "", Option("A"), Option("T TEST")))
        .unsafeRunSync()
    }
    ex.getMessage should equal("Cannot find asset with citable reference A")
  }

  "getDepartmentAndSeriesRows" should "return an error if the series reference doesn't match the input" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T"))
      .thenRespond(bodyMap("T"))
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/A TEST"))
      .thenRespond(bodyMap("T TEST"))

    val ex = intercept[Exception] {
      new DiscoveryService(baseUrl, backend, uuidIterator)
        .getDepartmentAndSeriesRows(Input("testBatch", "", "", Option("T"), Option("A TEST")))
        .unsafeRunSync()
    }
    ex.getMessage should equal("Cannot find asset with citable reference A TEST")
  }

  "getDepartmentAndSeriesRows" should "return an error if the discovery API returns an error" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError()).whenAnyRequest
      .thenRespondServerError()

    val ex = intercept[Exception] {
      new DiscoveryService(baseUrl, backend, uuidIterator)
        .getDepartmentAndSeriesRows(Input("testBatch", "", "", Option("T"), Option("A TEST")))
        .unsafeRunSync()
    }
    ex.getMessage should equal("statusCode: 500, response: Internal server error")
  }

  "getDepartmentAndSeriesRows" should "return an unknown department if the department is missing" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T TEST"))
      .thenRespond(bodyMap("T TEST"))

    val result = new DiscoveryService(baseUrl, backend, uuidIterator)
      .getDepartmentAndSeriesRows(Input("testBatch", "", "", None, Option("T TEST")))
      .unsafeRunSync()
    result.series.isDefined should equal(true)
    val department = result.department
    department("name").str should equal("Unknown")
    !department.value.contains("title") should equal(true)
    !department.value.contains("description") should equal(true)
  }

  "getDepartmentAndSeriesRows" should "return a department and an empty series if the series is missing" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T"))
      .thenRespond(bodyMap("T"))

    val result = new DiscoveryService(baseUrl, backend, uuidIterator)
      .getDepartmentAndSeriesRows(Input("testBatch", "", "", Option("T"), None))
      .unsafeRunSync()
    result.series.isDefined should equal(false)
    val department = result.department
    department("name").str should equal("T")
    department("title").str should equal("Test Title T")
    department("description").str should equal("TestDescription T 1          \nTestDescription T 2")
  }

  "getDepartmentAndSeriesRows" should "return an unknown department if the series and department are missing" in {
    val backend: SttpBackendStub[IO, Fs2Streams[IO]] = SttpBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())

    val result = new DiscoveryService(baseUrl, backend, uuidIterator)
      .getDepartmentAndSeriesRows(Input("testBatch", "", "", None, None))
      .unsafeRunSync()
    result.series.isDefined should equal(false)
    val department = result.department
    department("name").str should equal("Unknown")
    !department.value.contains("title") should equal(true)
    !department.value.contains("description") should equal(true)
  }
}
