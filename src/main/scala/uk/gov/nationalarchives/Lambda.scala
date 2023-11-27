package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import org.scanamo.generic.semiauto._
import org.scanamo._
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import ujson.{Null, Value, Obj, Str, Num}
import uk.gov.nationalarchives.Lambda.{Config, Input, StateOutput}
import uk.gov.nationalarchives.MetadataService._
import upickle.default
import upickle.default._

import java.io.{InputStream, OutputStream}
import java.util.UUID
import scala.collection.mutable

class Lambda extends RequestStreamHandler {
  val metadataService: MetadataService = MetadataService()
  val dynamo: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  val randomUuidGenerator: () => UUID = () => UUID.randomUUID()

  implicit val inputReader: Reader[Input] = macroR[Input]

  implicit def OptionReader[T: Reader]: Reader[Option[T]] = reader[Value].map[Option[T]] {
    case Null    => None
    case jsValue => Some(read[T](jsValue))
  }

  implicit def OptionWriter[T: Writer]: Writer[Option[T]] = writer[Value].comap {
    case Some(value) => write(value)
    case None        => Null
  }

  implicit val dynamoTableFormat: Typeclass[Obj] = new Typeclass[Obj] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, Obj] = {
      dynamoValue.asObject
        .map(_.toMap[String].map { valuesMap =>
          val jsonValuesMap = valuesMap.view.mapValues(Str)
          Obj(mutable.LinkedHashMap.newBuilder[String, Value].addAll(jsonValuesMap).result())
        })
        .getOrElse(Left(TypeCoercionError(new Exception("Dynamo object not found"))))
    }

    override def write(jsonObject: Obj): DynamoValue = {
      val dynamoValuesMap: Map[String, DynamoValue] = jsonObject.value.toMap.view
        .filterNot { case (_, value) => value.isNull }
        .mapValues {
          case Num(value) => DynamoValue.fromNumber[Long](value.toLong)
          case s          => DynamoValue.fromString(s.str)
        }
        .toMap
      DynamoValue.fromDynamoObject(DynamoObject(dynamoValuesMap))
    }
  }

  override def handleRequest(inputStream: InputStream, outputStream: OutputStream, context: Context): Unit = {
    val inputString = inputStream.readAllBytes().map(_.toChar).mkString
    val input = read[Input](inputString)
    for {
      config <- ConfigSource.default.loadF[IO, Config]()
      discoveryService <- DiscoveryService(config.discoveryApiUrl, randomUuidGenerator)
      departmentAndSeries <- discoveryService.getDepartmentAndSeriesRows(input)
      bagManifests <- metadataService.parseBagManifest(input)
      metadataJson <- metadataService.parseMetadataJson(input, departmentAndSeries, bagManifests)
      _ <- dynamo.writeItems(config.dynamoTableName, metadataJson)
    } yield {

      val typeToId: Map[Type, List[UUID]] = metadataJson
        .groupBy(jsonObj => typeFromString(jsonObj("type").str))
        .view
        .mapValues(_.map(jsonObj => UUID.fromString(jsonObj("id").str)))
        .toMap

      val stateData = StateOutput(
        input.batchId,
        input.s3Bucket,
        input.s3Prefix,
        typeToId.getOrElse(ArchiveFolder, Nil),
        typeToId.getOrElse(ContentFolder, Nil),
        typeToId.getOrElse(Asset, Nil)
      )
      outputStream.write(write(stateData).getBytes())
    }
  }.unsafeRunSync()

}
object Lambda {
  implicit val stateDataWriter: default.Writer[StateOutput] = macroW[StateOutput]
  case class StateOutput(batchId: String, s3Bucket: String, s3Prefix: String, archiveHierarchyFolders: List[UUID], contentFolders: List[UUID], contentAssets: List[UUID])
  case class Input(batchId: String, s3Bucket: String, s3Prefix: String, department: Option[String], series: Option[String])
  case class Config(dynamoTableName: String, discoveryApiUrl: String)
}
