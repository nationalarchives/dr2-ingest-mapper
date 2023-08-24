package uk.gov.nationalarchives

import cats.effect.IO
import cats.implicits._
import cats.effect.unsafe.implicits.global
import fs2.data.csv.{CsvRowDecoder, DecoderError, DecoderResult, NoneF, RowDecoder, RowF}
import org.mockito.stubbing.ScalaOngoingStubbing
import org.mockito.{ArgumentMatchers, MockitoSugar}
import org.reactivestreams.Publisher
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2, TableFor3}
import reactor.core.publisher.Flux
import uk.gov.nationalarchives.Lambda.Input
import uk.gov.nationalarchives.MetadataService.{AssetMetadata, BagitManifest, DepartmentSeries, DynamoTable, FileMetadata, FolderMetadata}

import java.nio.ByteBuffer
import java.util.UUID

class MetadataServiceTest extends AnyFlatSpec with MockitoSugar with TableDrivenPropertyChecks {
  case class Test(name: String, value: String)
  case class TestCsvRowDecoder() extends CsvRowDecoder[Test, String] {
    override def apply(row: RowF[Some, String]): DecoderResult[Test] = {
      val result = for {
        name <- row.as[String]("name")
        value <- row.as[String]("value")
      } yield Test(name, value)
      result.left.map(_ => new DecoderError("Error decoding csv"))
    }
  }
  case class TestRowDecoder() extends RowDecoder[Test] {
    override def apply(row: RowF[NoneF, Nothing]): DecoderResult[Test] =
      row.values.tail.headOption
        .map(tail => Right(Test(row.values.head, tail)))
        .getOrElse(Left(new DecoderError("Error decoding csv")))
  }
  implicit val testRowCsvDecoder: TestCsvRowDecoder = TestCsvRowDecoder()
  implicit val testRowDecoder: TestRowDecoder = TestRowDecoder()

  val testCsvWithHeaders = "name,value\ntestName1,testValue1\ntestName2,testValue2"
  val testCsvWithoutHeaders = "testName1 testValue1\ntestName2 testValue2"

  val invalidTestCsvWithHeaders = "invalid,header\ninvalidName,invalidValue"
  val invalidTestCsvWithoutHeaders = "invalidValue"

  def mockS3(responseText: String, returnError: Boolean = false): ScalaOngoingStubbing[IO[Publisher[ByteBuffer]]] = {
    val s3 = mock[DAS3Client[IO]]
    val stub = when(s3.download(ArgumentMatchers.eq("bucket"), ArgumentMatchers.eq("prefix/name")))
    if (returnError) {
      stub.thenThrow(new Exception("Key not found"))
    } else {
      stub.thenReturn(IO(Flux.just(ByteBuffer.wrap(responseText.getBytes))))
    }
  }

  val testCsvTable: TableFor3[String, String, String] = Table(
    ("id", "validCsv", "invalidCsv"),
    ("With", testCsvWithHeaders, invalidTestCsvWithHeaders),
    ("Without", testCsvWithoutHeaders, invalidTestCsvWithoutHeaders)
  )

  forAll(testCsvTable) { (id, validCsv, invalidCsv) =>
    def parseCsv(s3: DAS3Client[IO], input: Input): List[Test] = {
      val service = new MetadataService(s3)
      (if (id == "With") {
         service.parseCsvWithHeaders[Test](input, "name")
       } else {
         service.parseCsvWithoutHeaders[Test](input, "name")
       }).unsafeRunSync()
    }

    s"parseCsv${id}Headers" should "return the correct row values" in {
      val input = Input("testBatch", "bucket", "prefix/", Option("T"), Option("T TEST"))
      val s3 = mockS3(validCsv)
      val result = parseCsv(s3, input)

      result.size should equal(2)
      result.head.name should equal("testName1")
      result.head.value should equal("testValue1")
      result.last.name should equal("testName2")
      result.last.value should equal("testValue2")
    }

    s"parseCsv${id}Headers" should "return an error if the csv is in an unexpected format" in {
      val input = Input("testBatch", "bucket", "prefix/", Option("T"), Option("T TEST"))
      val s3 = mockS3(invalidCsv)
      val ex = intercept[DecoderError] {
        parseCsv(s3, input)
      }
      ex.getMessage should equal("Error decoding csv")
    }

    s"parseCsv${id}Headers" should "return an error if the s3 key is not found" in {
      val input = Input("testBatch", "bucket", "prefix/", Option("T"), Option("T TEST"))
      val s3 = mockS3(validCsv, returnError = true)
      val ex = intercept[Exception] {
        parseCsv(s3, input)
      }
      ex.getMessage should equal("Key not found")
    }
  }

  val departmentSeriesTable: TableFor2[UUID, Option[UUID]] = Table(
    ("departmentId", "seriesId"),
    (UUID.randomUUID(), Option(UUID.randomUUID())),
    (UUID.randomUUID(), None)
  )
  forAll(departmentSeriesTable) { (departmentId, seriesIdOpt) =>
    "metadataToDynamoTables" should s"return a list of tables with the correct prefix for department $departmentId and series ${seriesIdOpt.getOrElse("None")}" in {
      val s3 = mockS3(testCsvWithHeaders)

      def table(id: UUID, tableType: String, parentPath: String) =
        DynamoTable("batchId", id, parentPath, tableType, "Folder", s"$tableType Title", s"$tableType Description")

      val batchId = "batchId"
      val folderId = UUID.randomUUID()
      val assetId = UUID.randomUUID()
      val fileIdOne = UUID.randomUUID()
      val fileIdTwo = UUID.randomUUID()
      val departmentTable = table(departmentId, "department", "")
      val seriesTable = seriesIdOpt.map(id => table(id, "series", departmentId.toString))
      val departmentAndSeries = DepartmentSeries(departmentTable, seriesTable)
      val folderMetadata = List(FolderMetadata(folderId, "", "folderName", "folderTitle"))
      val assetMetadata = List(AssetMetadata(assetId, folderId.toString, "assetTitle"))
      val fileMetadata = List(
        FileMetadata(fileIdOne, s"$folderId/$assetId", "fileName1.txt", 1, "fileTitle1"),
        FileMetadata(fileIdTwo, s"$folderId/$assetId", "fileName2.pdf", 2, "fileTitle2")
      )
      val bagitManifests: List[BagitManifest] = fileMetadata.map(fm => BagitManifest(s"${fm.name}checksum", s"data/${fm.identifier}"))
      val result =
        new MetadataService(s3).metadataToDynamoTables(batchId, departmentAndSeries, folderMetadata ++ assetMetadata ++ fileMetadata, bagitManifests).unsafeRunSync()

      result.size should equal(5 + seriesIdOpt.size)

      def checkTableRows(ids: List[UUID], expectedSize: Int, expectedTable: DynamoTable) = {
        val rows = result.filter(r => ids.contains(r.id))
        rows.size should equal(expectedSize)
        rows.map { row =>
          ids.contains(row.id) should be(true)
          row.name should equal(expectedTable.name)
          row.title should equal(expectedTable.title)
          row.parentPath should equal(expectedTable.parentPath)
          row.batchId should equal(expectedTable.batchId)
          row.description should equal(expectedTable.description)
          row.fileSize should equal(expectedTable.fileSize)
          row.`type` should equal(expectedTable.`type`)
        }
      }
      val prefix = s"$departmentId${seriesIdOpt.map(id => s"/$id").getOrElse("")}"
      checkTableRows(List(departmentId), 1, DynamoTable(batchId, departmentId, "", "department", "Folder", "department Title", "department Description"))
      seriesIdOpt.map(seriesId =>
        checkTableRows(List(seriesId), 1, DynamoTable(batchId, seriesId, departmentId.toString, "series", "Folder", "series Title", "series Description"))
      )
      checkTableRows(List(folderId), 1, DynamoTable(batchId, folderId, s"$prefix", folderMetadata.head.name, "Folder", folderMetadata.head.title, ""))
      checkTableRows(List(assetId), 1, DynamoTable(batchId, assetId, s"$prefix/$folderId", assetMetadata.head.title, "Asset", "", ""))
      checkTableRows(
        List(fileIdOne),
        1,
        DynamoTable(
          batchId,
          assetId,
          s"$prefix/$folderId/$assetId",
          fileMetadata.head.name,
          "File",
          fileMetadata.head.title,
          "",
          Option(fileMetadata.head.fileSize),
          Option(s"${fileMetadata.head.name}checksum"),
          Option(".txt")
        )
      )
      checkTableRows(
        List(fileIdTwo),
        1,
        DynamoTable(
          batchId,
          assetId,
          s"$prefix/$folderId/$assetId",
          fileMetadata.last.name,
          "File",
          fileMetadata.last.title,
          "",
          Option(fileMetadata.last.fileSize),
          Option(s"${fileMetadata.head.name}checksum"),
          Option(".pdf")
        )
      )
    }
  }

}
