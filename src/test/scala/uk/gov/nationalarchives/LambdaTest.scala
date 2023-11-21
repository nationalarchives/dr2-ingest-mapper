package uk.gov.nationalarchives

import cats.effect.IO
import com.github.tomakehurst.wiremock.WireMockServer
import com.github.tomakehurst.wiremock.client.WireMock._
import org.apache.commons.io.output.ByteArrayOutputStream
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient
import software.amazon.awssdk.services.s3.S3AsyncClient
import uk.gov.nationalarchives.Lambda._
import uk.gov.nationalarchives.TestUtils._
import uk.gov.nationalarchives.MetadataService._
import upickle.default
import upickle.default._

import java.io.ByteArrayInputStream
import java.net.URI
import java.util.UUID
import scala.jdk.CollectionConverters.ListHasAsScala

class LambdaTest extends AnyFlatSpec with MockitoSugar with BeforeAndAfterEach {

  implicit val stateDataReader: default.Reader[StateOutput] = macroR[StateOutput]

  override def beforeEach(): Unit = {
    dynamoServer.resetAll()
    dynamoServer.start()
    s3Server.resetAll()
    s3Server.start()
    discoveryServer.resetAll()
    discoveryServer.start()
  }

  val s3Server = new WireMockServer(9003)
  val dynamoServer = new WireMockServer(9005)
  val discoveryServer = new WireMockServer(9004)
  val inputBucket = "input"
  val uuids: List[String] = List(
    "c7e6b27f-5778-4da8-9b83-1b64bbccbd03",
    "61ac0166-ccdf-48c4-800f-29e5fba2efda"
  )
  private def defaultInputStream: ByteArrayInputStream = {
    val inJson =
      s"""{
         |  "batchId": "TEST",
         |  "s3Bucket": "$inputBucket",
         |  "s3Prefix" : "TEST/",
         |  "department": "A",
         |  "series": "A 1"
         |}""".stripMargin
    new ByteArrayInputStream(inJson.getBytes())
  }

  private def stubValidNetworkRequests(dynamoTable: String = "test") = {
    val folderIdentifier = UUID.randomUUID()
    val assetIdentifier = UUID.randomUUID()
    val docxIdentifier = UUID.randomUUID()
    val metadataFileIdentifier = UUID.randomUUID()
    val metadata =
      s"""[{"id":"$folderIdentifier","parentId":null,"title":"TestTitle","type":"ArchiveFolder","name":"TestName","fileSize":null, "customMetadataAttribute2": "customMetadataValue2"},
        |{"id":"$assetIdentifier","parentId":"$folderIdentifier","title":"TestAssetTitle","type":"Asset","name":"TestAssetName","fileSize":null},
        |{"id":"$docxIdentifier","parentId":"$assetIdentifier","title":"Test","type":"File","name":"Test.docx","fileSize":1, "customMetadataAttribute1": "customMetadataValue1"},
        |{"id":"$metadataFileIdentifier","parentId":"$assetIdentifier","title":"","type":"File","name":"TEST-metadata.json","fileSize":2}]
        |""".stripMargin.replaceAll("\n", "")

    val manifestData: String =
      s"""checksumdocx data/$docxIdentifier
         |checksummetadata data/$metadataFileIdentifier
         |""".stripMargin

    stubNetworkRequests(dynamoTable, metadata, manifestData)
    (folderIdentifier, assetIdentifier, docxIdentifier, metadataFileIdentifier)
  }

  private def stubInvalidNetworkRequests(dynamoTable: String = "test"): Unit = {
    val metadata: String = "{}"

    val manifestData: String = ""

    stubNetworkRequests(dynamoTable, metadata, manifestData)
  }

  private def stubNetworkRequests(dynamoTableName: String = "test", metadata: String, manifestData: String): Unit = {
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.RequestItems", containing(dynamoTableName)))
        .willReturn(ok())
    )

    List(
      ("metadata.json", metadata),
      ("manifest-sha256.txt", manifestData)
    ).map { case (name, responseCsv) =>
      s3Server.stubFor(
        head(urlEqualTo(s"/TEST/$name"))
          .willReturn(
            ok()
              .withHeader("Content-Length", responseCsv.getBytes.length.toString)
              .withHeader("ETag", "abcde")
          )
      )
      s3Server.stubFor(
        get(urlEqualTo(s"/TEST/$name"))
          .willReturn(ok.withBody(responseCsv.getBytes))
      )
    }

    List("A", "A 1").foreach { col =>
      val body =
        s"""{
           |  "assets": [
           |    {
           |      "citableReference": "$col",
           |      "scopeContent": {
           |        "description": "<scopecontent><head>Head</head><p>TestDescription$col with &#48</p></scopecontent>"
           |      },
           |      "title": "<unittitle>Test Title $col</unittitle>"
           |    }
           |  ]
           |}
           |""".stripMargin

      discoveryServer.stubFor(
        get(urlEqualTo(s"/API/records/v1/collection/${col.replace(" ", "%20")}"))
          .willReturn(okJson(body))
      )
    }
  }

  private def checkDynamoItems(tableRequestItems: List[DynamoTableItem], expectedTable: DynamoTable) = {
    val items = tableRequestItems
      .filter(_.PutRequest.Item.items("id").asInstanceOf[DynamoSRequestField].S == expectedTable.id.toString)
      .map(_.PutRequest.Item)
    items.size should equal(1)
    val item = items.head.items
    def strOpt(name: String) = item.get(name).map(_.asInstanceOf[DynamoSRequestField].S)
    def str(name: String) = strOpt(name).getOrElse("")
    str("id") should equal(expectedTable.id.toString)
    str("name") should equal(expectedTable.name)
    str("title") should equal(expectedTable.title)
    expectedTable.id_Code.map(id_Code => str("id_Code") should equal(id_Code))
    str("parentPath") should equal(expectedTable.parentPath)
    str("batchId") should equal(expectedTable.batchId)
    str("description") should equal(expectedTable.description)
    item.get("fileSize").map(_.asInstanceOf[DynamoNRequestField].N) should equal(expectedTable.fileSize)
    str("type") should equal(expectedTable.`type`.toString)
    strOpt("customMetadataAttribute1") should equal(expectedTable.customMetadataAttribute1)
    strOpt("customMetadataAttribute2") should equal(expectedTable.customMetadataAttribute2)
  }

  case class IngestMapperTest() extends Lambda {
    val creds: StaticCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test"))
    private val asyncS3Client: S3AsyncClient = S3AsyncClient
      .crtBuilder()
      .endpointOverride(URI.create("http://localhost:9003"))
      .credentialsProvider(creds)
      .region(Region.EU_WEST_2)
      .build()
    private val asyncDynamoClient: DynamoDbAsyncClient = DynamoDbAsyncClient
      .builder()
      .endpointOverride(URI.create("http://localhost:9005"))
      .region(Region.EU_WEST_2)
      .credentialsProvider(creds)
      .build()
    val uuidsIterator: Iterator[String] = uuids.iterator
    override val metadataService: MetadataService = new MetadataService(DAS3Client[IO](asyncS3Client))
    override val dynamo: DADynamoDBClient[IO] = new DADynamoDBClient[IO](asyncDynamoClient)
    override val randomUuidGenerator: () => UUID = () => UUID.fromString(uuidsIterator.next())
  }
  "handleRequest" should "return the correct values from the lambda" in {
    val (folderIdentifier, assetIdentifier, _, _) = stubValidNetworkRequests()

    val os = new ByteArrayOutputStream()
    IngestMapperTest().handleRequest(defaultInputStream, os, null)
    val stateData = read[StateOutput](os.toByteArray.map(_.toChar).mkString)
    val archiveFolders = stateData.archiveHierarchyFolders
    archiveFolders.size should be(3)
    archiveFolders.contains(folderIdentifier) should be(true)
    List(folderIdentifier, UUID.fromString(uuids.tail.head), UUID.fromString(uuids.head)).equals(archiveFolders) should be(true)

    stateData.contentFolders.isEmpty should be(true)

    stateData.contentAssets.size should be(1)
    stateData.contentAssets.head should equal(assetIdentifier)
  }

  "handleRequest" should "write the correct values to dynamo" in {
    val (folderIdentifier, assetIdentifier, docxIdentifier, metadataIdentifier) = stubValidNetworkRequests()
    val os = new ByteArrayOutputStream()
    IngestMapperTest().handleRequest(defaultInputStream, os, null)
    val dynamoRequestBodies = dynamoServer.getAllServeEvents.asScala.map(e => read[DynamoRequestBody](e.getRequest.getBodyAsString))
    dynamoRequestBodies.length should equal(1)
    val tableRequestItems = dynamoRequestBodies.head.RequestItems.test

    tableRequestItems.length should equal(6)
    checkDynamoItems(
      tableRequestItems,
      DynamoTable("TEST", UUID.fromString(uuids.head), "", "A", ArchiveFolder, "Test Title A", "TestDescriptionA with 0", Some("A"))
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoTable("TEST", UUID.fromString(uuids.tail.head), uuids.head, "A 1", ArchiveFolder, "Test Title A 1", "TestDescriptionA 1 with 0", Some("A 1"))
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoTable(
        "TEST",
        folderIdentifier,
        s"${uuids.head}/${uuids.tail.head}",
        "TestName",
        ArchiveFolder,
        "TestTitle",
        "",
        None,
        customMetadataAttribute2 = Option("customMetadataValue2")
      )
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoTable("TEST", assetIdentifier, s"${uuids.head}/${uuids.tail.head}/$folderIdentifier", "TestAssetName", Asset, "TestAssetTitle", "", None)
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoTable(
        "TEST",
        docxIdentifier,
        s"${uuids.head}/${uuids.tail.head}/$folderIdentifier/$assetIdentifier",
        "Test.docx",
        File,
        "Test",
        "",
        None,
        Option(1),
        customMetadataAttribute1 = Option("customMetadataValue1")
      )
    )
    checkDynamoItems(
      tableRequestItems,
      DynamoTable(
        "TEST",
        metadataIdentifier,
        s"${uuids.head}/${uuids.tail.head}/$folderIdentifier/$assetIdentifier",
        "TEST-metadata.json",
        File,
        "",
        "",
        None,
        Option(2),
        Option("checksum"),
        Option("txt")
      )
    )
  }

  "handleRequest" should "return an error if the discovery api is unavailable" in {
    stubValidNetworkRequests()
    discoveryServer.stop()
    val os = new ByteArrayOutputStream()
    val ex = intercept[Exception] {
      IngestMapperTest().handleRequest(defaultInputStream, os, null)
    }

    ex.getMessage should equal("Exception when sending request: GET http://localhost:9004/API/records/v1/collection/A")
  }

  "handleRequest" should "return an error if the input files are not stored in S3" in {
    stubValidNetworkRequests()
    val inJson =
      s"""{
         |  "batchId": "TEST",
         |  "s3Bucket": "$inputBucket",
         |  "s3Prefix" : "INVALID/",
         |  "department": "A",
         |  "series": "A 1"
         |}""".stripMargin
    val is = new ByteArrayInputStream(inJson.getBytes())
    val os = new ByteArrayOutputStream()
    val ex = intercept[Exception] {
      IngestMapperTest().handleRequest(is, os, null)
    }

    ex.getMessage should equal("null (Service: S3, Status Code: 404, Request ID: null)")
  }

  "handleRequest" should "return an error if the dynamo table doesn't exist" in {
    stubValidNetworkRequests("invalidTable")
    val os = new ByteArrayOutputStream()
    val ex = intercept[Exception] {
      IngestMapperTest().handleRequest(defaultInputStream, os, null)
    }

    ex.getMessage should equal("Service returned HTTP status code 404 (Service: DynamoDb, Status Code: 404, Request ID: null)")
  }

  "handleRequest" should "return an error if the bag files from S3 are invalid" in {
    stubInvalidNetworkRequests()
    val os = new ByteArrayOutputStream()
    val ex = intercept[Exception] {
      IngestMapperTest().handleRequest(defaultInputStream, os, null)
    }

    ex.getMessage should equal("Expected ujson.Arr (data: {})")
  }
}
