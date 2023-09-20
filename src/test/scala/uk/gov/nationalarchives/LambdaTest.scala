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
import ujson.Obj
import uk.gov.nationalarchives.Lambda._
import uk.gov.nationalarchives.MetadataService.{Asset, DynamoTable, File, Folder}
import upickle.default
import upickle.default._

import java.io.ByteArrayInputStream
import java.net.URI
import java.util.UUID
import scala.jdk.CollectionConverters.ListHasAsScala

class LambdaTest extends AnyFlatSpec with MockitoSugar with BeforeAndAfterEach {

  implicit val stateDataReader: default.Reader[StateData] = macroR[StateData]

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
  implicit val sRequestFieldReader: Reader[DynamoSRequestField] = macroR[DynamoSRequestField]
  implicit val nRequestFieldReader: Reader[DynamoNRequestField] = macroR[DynamoNRequestField]
  implicit val itemReader: Reader[DynamoItem] = reader[Obj].map[DynamoItem] { json =>
    DynamoItem(
      read[DynamoSRequestField](json("batchId")),
      read[DynamoSRequestField](json("id")),
      json.obj.get("parentPath").map(pp => read[DynamoSRequestField](pp)).getOrElse(DynamoSRequestField("")),
      read[DynamoSRequestField](json("name")),
      read[DynamoSRequestField](json("type")),
      if (json.obj.contains("fileSize")) Option(read[DynamoNRequestField](json("fileSize"))) else None,
      json.obj.get("title").map(pp => read[DynamoSRequestField](pp)).getOrElse(DynamoSRequestField("")),
      json.obj.get("description").map(pp => read[DynamoSRequestField](pp)).getOrElse(DynamoSRequestField(""))
    )
  }
  implicit val tableItemReader: Reader[DynamoTableItem] = macroR[DynamoTableItem]
  implicit val putRequestReader: Reader[DynamoPutRequest] = macroR[DynamoPutRequest]
  implicit val requestItemReader: Reader[DynamoRequestItem] = macroR[DynamoRequestItem]
  implicit val requestBodyReader: Reader[DynamoRequestBody] = macroR[DynamoRequestBody]
  case class DynamoSRequestField(S: String)
  case class DynamoNRequestField(N: Long)
  case class DynamoItem(
      batchId: DynamoSRequestField,
      id: DynamoSRequestField,
      parentPath: DynamoSRequestField,
      name: DynamoSRequestField,
      `type`: DynamoSRequestField,
      fileSize: Option[DynamoNRequestField],
      title: DynamoSRequestField,
      description: DynamoSRequestField
  )
  case class DynamoTableItem(PutRequest: DynamoPutRequest)
  case class DynamoPutRequest(Item: DynamoItem)
  case class DynamoRequestItem(test: List[DynamoTableItem])
  case class DynamoRequestBody(RequestItems: DynamoRequestItem)

  private def stubValidNetworkRequests(dynamoTable: String = "test") = {
    val folderIdentifier = UUID.randomUUID()
    val assetIdentifier = UUID.randomUUID()
    val docxIdentifier = UUID.randomUUID()
    val metadataFileIdentifier = UUID.randomUUID()
    val folderMetadata: String =
      s"""identifier,parentPath,name,title
         |$folderIdentifier,,TestName,TestTitle""".stripMargin

    val assetMetadata: String =
      s"""identifier,parentPath,title
         |$assetIdentifier,$folderIdentifier,TestAssetTitle""".stripMargin

    val fileMetadata: String =
      s"""identifier,parentPath,name,fileSize,title
         |$docxIdentifier,$folderIdentifier/$assetIdentifier,Test.docx,1,TestTitle
         |$metadataFileIdentifier,$folderIdentifier/$assetIdentifier,TEST-metadata.json,2,
         |""".stripMargin

    val manifestData: String =
      s"""checksumdocx data/$docxIdentifier
         |checksummetadata data/$metadataFileIdentifier
         |""".stripMargin

    stubNetworkRequests(dynamoTable, folderMetadata, assetMetadata, fileMetadata, manifestData)
    (folderIdentifier, assetIdentifier, docxIdentifier, metadataFileIdentifier)
  }

  private def stubInvalidNetworkRequests(dynamoTable: String = "test"): Unit = {
    val folderMetadata: String =
      s"""invalidFolder,headers
         |invalid,values""".stripMargin

    val assetMetadata: String =
      s"""invalidAsset,headers
         |invalid,values""".stripMargin

    val fileMetadata: String =
      s"""invalidFile,headers
         |invalid,values""".stripMargin

    val manifestData: String = ""

    stubNetworkRequests(dynamoTable, folderMetadata, assetMetadata, fileMetadata, manifestData)
  }

  private def stubNetworkRequests(dynamoTableName: String = "test", folderMetadata: String, assetMetadata: String, fileMetadata: String, manifestData: String): Unit = {
    dynamoServer.stubFor(
      post(urlEqualTo("/"))
        .withRequestBody(matchingJsonPath("$.RequestItems", containing(dynamoTableName)))
        .willReturn(ok())
    )

    List(
      ("folder-metadata.csv", folderMetadata),
      ("asset-metadata.csv", assetMetadata),
      ("file-metadata.csv", fileMetadata),
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
    val items = tableRequestItems.filter(_.PutRequest.Item.id.S == expectedTable.id.toString).map(_.PutRequest.Item)
    items.size should equal(1)
    val item = items.head
    item.id.S should equal(expectedTable.id.toString)
    item.name.S should equal(expectedTable.name)
    item.title.S should equal(expectedTable.title)
    item.parentPath.S should equal(expectedTable.parentPath)
    item.batchId.S should equal(expectedTable.batchId)
    item.description.S should equal(expectedTable.description)
    item.fileSize.map(_.N) should equal(expectedTable.fileSize)
    item.`type`.S should equal(expectedTable.`type`.toString)
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
    val stateData = read[StateData](os.toByteArray.map(_.toChar).mkString)
    val archiveFolders = stateData.archiveHierarchyFolders
    archiveFolders.size should be(3)
    archiveFolders.contains(folderIdentifier) should be(true)
    archiveFolders.containsSlice(uuids.map(UUID.fromString)) should be(true)

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
    checkDynamoItems(tableRequestItems, DynamoTable("TEST", UUID.fromString(uuids.head), "", "A", Folder, "Test Title A", "TestDescriptionA with 0"))
    checkDynamoItems(tableRequestItems, DynamoTable("TEST", UUID.fromString(uuids.tail.head), uuids.head, "A 1", Folder, "Test Title A 1", "TestDescriptionA 1 with 0"))
    checkDynamoItems(tableRequestItems, DynamoTable("TEST", folderIdentifier, s"${uuids.head}/${uuids.tail.head}", "TestName", Folder, "TestTitle", ""))
    checkDynamoItems(tableRequestItems, DynamoTable("TEST", assetIdentifier, s"${uuids.head}/${uuids.tail.head}/$folderIdentifier", "TestAssetTitle", Asset, "", ""))
    checkDynamoItems(
      tableRequestItems,
      DynamoTable("TEST", docxIdentifier, s"${uuids.head}/${uuids.tail.head}/$folderIdentifier/$assetIdentifier", "Test.docx", File, "TestTitle", "", Option(1))
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

    ex.getMessage should equal("unknown column name 'identifier' in line 2")
  }
}
