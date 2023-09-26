package uk.gov.nationalarchives

import cats.effect.IO
import fs2.interop.reactivestreams._
import fs2.{Chunk, Pipe, Stream, text}
import uk.gov.nationalarchives.Lambda.Input
import uk.gov.nationalarchives.MetadataService._
import ujson._

import java.util.UUID

class MetadataService(s3: DAS3Client[IO]) {
  lazy private val bufferSize = 1024 * 5

  private def getParentPaths(json: Value.Value): Map[UUID, String] = {
    val idToParentId = json.arr.toList.map { eachEntry =>
      UUID.fromString(eachEntry("id").str) -> eachEntry("parentId").strOpt.map(UUID.fromString)
    }.toMap

    def searchParentIds(parentIdOpt: Option[UUID]): String = {
      val nextParent = parentIdOpt.flatMap(idToParentId.get).flatten
      val thisParent = parentIdOpt.map(_.toString).getOrElse("")
      if (nextParent.isEmpty) {
        thisParent
      } else {
        searchParentIds(nextParent) + s"/$thisParent"
      }
    }
    idToParentId.map { case (id, parentIdOpt) =>
      id -> searchParentIds(parentIdOpt)
    }
  }

  def parseMetadataJson(input: Input, departmentAndSeries: DepartmentAndSeriesTableData, bagitManifests: List[BagitManifest]): IO[List[Obj]] = {
    parseFileFromS3(
      input,
      "metadata.json",
      s =>
        s.flatMap { jsonString =>
          val fileIdToChecksum: Map[UUID, String] = bagitManifests.map(bm => UUID.fromString(bm.filePath.stripPrefix("data/")) -> bm.checksum).toMap
          val json = read(jsonString)
          val pathPrefix = departmentAndSeries.series
            .map(series => s"${departmentAndSeries.department("id").str}/${series("id").str}")
            .getOrElse(s"${departmentAndSeries.department("id").str}")
          val parentPaths = getParentPaths(json)
          Stream.emits {
            json.arr.toList.map { eachEntry =>
              val id = UUID.fromString(eachEntry("id").str)
              val name = eachEntry("name").strOpt
              val parentPath = parentPaths(id)
              val path = if (parentPath.isEmpty) pathPrefix else s"$pathPrefix/${parentPath.stripPrefix("/")}"
              val checksum = fileIdToChecksum.get(id).map(Str).getOrElse(Null)
              val fileExtension =
                if (eachEntry("type").str == "File")
                  name
                    .flatMap(n => n.split("\\.").lastOption)
                    .map(Str)
                    .getOrElse(Null)
                else Null
              val objectMap =
                Map("batchId" -> Str(input.batchId), "parentPath" -> Str(path), "checksum" -> checksum, "fileExtension" -> fileExtension) ++ eachEntry.obj.view
                  .filterKeys(_ != "parentId")
                  .toMap
              Obj.from(objectMap)
            } ++ departmentAndSeries.series.toList ++ List(departmentAndSeries.department)
          }
        }
    )
  }

  def parseBagManifest(input: Input): IO[List[BagitManifest]] = {
    parseFileFromS3(
      input,
      "manifest-sha256.txt",
      file => {
        file.flatMap { bagitManifestString =>
          Stream.emits {
            bagitManifestString
              .split("\n")
              .map { row =>
                val eachColumn = row.split(" ")
                BagitManifest(eachColumn.head, eachColumn.last)
              }
              .toList
          }
        }
      }
    )
  }

  private def parseFileFromS3[T](input: Input, name: String, decoderPipe: Pipe[IO, String, T]): IO[List[T]] = {
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
}
object MetadataService {
  def typeFromString(typeString: String): Type = typeString match {
    case "ArchiveFolder" => ArchiveFolder
    case "ContentFolder" => ContentFolder
    case "Asset"         => Asset
    case "File"          => File
  }

  sealed trait Type {
    override def toString: String = this match {
      case ArchiveFolder => "ArchiveFolder"
      case ContentFolder => "ContentFolder"
      case Asset         => "Asset"
      case File          => "File"
    }
  }
  case object ArchiveFolder extends Type
  case object ContentFolder extends Type
  case object Asset extends Type
  case object File extends Type

  sealed trait Metadata {
    def id: UUID
    def parentPath: String
    def title: String
  }

  case class BagitManifest(checksum: String, filePath: String)

  case class DepartmentAndSeriesTableData(department: Obj, series: Option[Obj])

  def apply(): MetadataService = {
    val s3 = DAS3Client[IO]()
    new MetadataService(s3)
  }
}
