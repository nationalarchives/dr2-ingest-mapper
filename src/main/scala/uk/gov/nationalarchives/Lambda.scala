package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import fs2.data.csv._
import fs2.data.csv.generic.semiauto._
import org.scanamo.generic.semiauto._
import org.scanamo.{DynamoFormat, TypeCoercionError}
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import ujson.{Null, Value}
import uk.gov.nationalarchives.Lambda.{Config, _}
import uk.gov.nationalarchives.MetadataService._
import upickle.default
import upickle.default._

import java.io.{InputStream, OutputStream}
import java.util.UUID

class Lambda extends RequestStreamHandler {
  val metadataService: MetadataService = MetadataService()
  val dynamo: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  val randomUuidGenerator: () => UUID = () => UUID.randomUUID()

  implicit val inputReader: Reader[Input] = macroR[Input]
  implicit val folderMetadataRowDecoder: CsvRowDecoder[FolderMetadata, String] = deriveCsvRowDecoder
  implicit val assetMetadataRowDecoder: CsvRowDecoder[AssetMetadata, String] = deriveCsvRowDecoder
  implicit val fileMetadataRowDecoder: CsvRowDecoder[FileMetadata, String] = deriveCsvRowDecoder
  implicit val bagManifestRowDecoder: RowDecoder[BagitManifest] = deriveRowDecoder

  implicit def OptionReader[T: Reader]: Reader[Option[T]] = reader[Value].map[Option[T]] {
    case Null    => None
    case jsValue => Some(read[T](jsValue))
  }

  implicit def OptionWriter[T: Writer]: Writer[Option[T]] = writer[Value].comap {
    case Some(value) => write(value)
    case None        => Null
  }

  implicit val typeFormat: Typeclass[Type] = DynamoFormat.xmap[Type, String](
    {
      case "Folder"   => Right(Folder())
      case "Asset"    => Right(Asset())
      case "File"     => Right(File())
      case typeString => Left(TypeCoercionError(new Exception(s"Type $typeString not found")))
    },
    typeCaseClass => typeCaseClass.getClass.getSimpleName
  )

  implicit val dynamoTableFormat: Typeclass[DynamoTable] = deriveDynamoFormat[DynamoTable]

  override def handleRequest(inputStream: InputStream, outputStream: OutputStream, context: Context): Unit = {
    val inputString = inputStream.readAllBytes().map(_.toChar).mkString
    val input = read[Input](inputString)
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      discoveryService <- DiscoveryService(config.discoveryApiUrl, randomUuidGenerator)
      departmentAndSeries <- discoveryService.getDepartmentAndSeriesRows(input)
      folderMetadata <- metadataService.parseCsvWithHeaders[FolderMetadata](input, "folder-metadata.csv")
      assetMetadata <- metadataService.parseCsvWithHeaders[AssetMetadata](input, "asset-metadata.csv")
      fileMetadata <- metadataService.parseCsvWithHeaders[FileMetadata](input, "file-metadata.csv")
      bagManifests <- metadataService.parseCsvWithoutHeaders[BagitManifest](input, "manifest-sha256.txt")
      entries <- metadataService.metadataToDynamoTables(input.batchId, departmentAndSeries, folderMetadata ++ assetMetadata ++ fileMetadata, bagManifests)
      _ <- dynamo.writeItems(config.dynamoTableName, entries)
    } yield {
      val folderMetadataIdsWhereTitleAndNameNotSame: List[UUID] = folderMetadata.filter(fm => fm.title != fm.name).map(_.identifier)
      val departmentAndSeriesIds: List[UUID] = departmentAndSeries.department.id :: departmentAndSeries.series.map(_.id).toList

      val stateData = StateData(
        input.batchId,
        input.s3Bucket,
        input.s3Prefix,
        folderMetadataIdsWhereTitleAndNameNotSame ++ departmentAndSeriesIds,
        folderMetadata.filter(fm => fm.title == fm.name).map(_.identifier),
        assetMetadata.map(_.identifier)
      )
      outputStream.write(write(stateData).getBytes())
    }
  }.unsafeRunSync()

}
object Lambda {
  implicit val stateDataWriter: default.Writer[StateData] = macroW[StateData]
  case class StateData(batchId: String, s3Bucket: String, s3Prefix: String, archiveHierarchyFolders: List[UUID], contentFolders: List[UUID], contentAssets: List[UUID])
  case class Input(batchId: String, s3Bucket: String, s3Prefix: String, department: Option[String], series: Option[String])
  case class Config(dynamoTableName: String, discoveryApiUrl: String)
}
