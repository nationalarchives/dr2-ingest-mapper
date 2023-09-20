package uk.gov.nationalarchives

import cats.effect.IO
import fs2.data.csv._
import fs2.interop.reactivestreams._
import fs2.{Chunk, Pipe, Stream, text}
import uk.gov.nationalarchives.Lambda.Input
import uk.gov.nationalarchives.MetadataService._

import java.util.UUID

class MetadataService(s3: DAS3Client[IO]) {
  lazy private val bufferSize = 1024 * 5

  def parseCsvWithHeaders[T](input: Input, name: String)(implicit decoder: CsvRowDecoder[T, String]): IO[List[T]] = {
    parseCsv(input, name, decodeUsingHeaders[T]())
  }

  def parseCsvWithoutHeaders[T](input: Input, name: String)(implicit decoder: RowDecoder[T]): IO[List[T]] = {
    parseCsv(input, name, decodeWithoutHeaders[T](' '))
  }

  private def parseCsv[T](input: Input, name: String, decoderPipe: Pipe[IO, String, T]): IO[List[T]] = {
    for {
      pub <- s3.download(input.s3Bucket, s"${input.s3Prefix}$name")
      csvString <- pub
        .toStreamBuffered[IO](bufferSize)
        .flatMap(bf => Stream.chunk(Chunk.byteBuffer(bf)))
        .through(text.utf8.decode)
        .through(decoderPipe)
        .compile
        .toList
    } yield csvString
  }

  private def ifEmptyOtherwise(str: String, otherwise: String): String =
    if (str.isEmpty || Option(str).isEmpty) otherwise else str

  def metadataToDynamoTables(
      batchId: String,
      departmentAndSeries: DepartmentAndSeriesTableData,
      metadata: List[Metadata],
      bagitManifests: List[BagitManifest]
  ): IO[List[DynamoTable]] = {
    val fileIdToChecksum: Map[UUID, String] = bagitManifests.map(bm => UUID.fromString(bm.filePath.stripPrefix("data/")) -> bm.checksum).toMap
    val pathPrefix = departmentAndSeries.series
      .map(series => s"${departmentAndSeries.department.id}/${series.id}")
      .getOrElse(s"${departmentAndSeries.department.id}")
    IO {
      departmentAndSeries.department ::
        metadata.map {
          case AssetMetadata(identifier, parentPath, title) =>
            DynamoTable(batchId, identifier, s"$pathPrefix/${parentPath.stripPrefix("/")}", "", Asset, title, "")
          case FileMetadata(identifier, parentPath, name, fileSize, title) =>
            DynamoTable(
              batchId,
              identifier,
              s"$pathPrefix/${parentPath.stripPrefix("/")}",
              name,
              File,
              ifEmptyOtherwise(title, name),
              "",
              Option(fileSize),
              fileIdToChecksum.get(identifier),
              name.split("\\.").lastOption
            )
          case FolderMetadata(identifier, parentPath, name, title) =>
            val path = if (parentPath.isEmpty) pathPrefix else s"$pathPrefix/${parentPath.stripPrefix("/")}"
            DynamoTable(batchId, identifier, path, name, Folder, ifEmptyOtherwise(title, name), "")
        } ++ departmentAndSeries.series.toList

    }
  }
}
object MetadataService {
  sealed trait Type {
    override def toString: String = this match {
      case Folder => "Folder"
      case Asset  => "Asset"
      case File   => "File"
    }
  }
  case object Folder extends Type
  case object Asset extends Type
  case object File extends Type

  sealed trait Metadata {
    def identifier: UUID
    def parentPath: String
    def title: String
  }

  case class DynamoTable(
      batchId: String,
      id: UUID,
      parentPath: String,
      name: String,
      `type`: Type,
      title: String,
      description: String,
      fileSize: Option[Long] = None,
      checksumSha256: Option[String] = None,
      fileExtension: Option[String] = None
  )

  case class FolderMetadata(identifier: UUID, parentPath: String, name: String, title: String) extends Metadata

  case class AssetMetadata(identifier: UUID, parentPath: String, title: String) extends Metadata

  case class FileMetadata(identifier: UUID, parentPath: String, name: String, fileSize: Long, title: String) extends Metadata

  case class BagitManifest(checksum: String, filePath: String)

  case class DepartmentAndSeriesTableData(department: DynamoTable, series: Option[DynamoTable])

  def apply(): MetadataService = {
    val s3 = DAS3Client[IO]()
    new MetadataService(s3)
  }
}
