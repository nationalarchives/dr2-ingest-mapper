package uk.gov.nationalarchives

import cats.effect.{IO, Resource}
import cats.implicits._
import sttp.capabilities.fs2.Fs2Streams
import sttp.client3.httpclient.fs2.HttpClientFs2Backend
import sttp.client3.upicklejson.asJson
import sttp.client3.{SttpBackend, UriContext, basicRequest}
import uk.gov.nationalarchives.DiscoveryService._
import uk.gov.nationalarchives.Lambda.Input
import uk.gov.nationalarchives.MetadataService.{DepartmentAndSeriesTableData, DynamoTable, Folder}
import upickle.default._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.UUID
import javax.xml.transform.TransformerFactory
import javax.xml.transform.stream.{StreamResult, StreamSource}
import scala.xml.XML

class DiscoveryService(discoveryBaseUrl: String, backend: SttpBackend[IO, Fs2Streams[IO]], randomUuidGenerator: () => UUID) {
  private val folder = Folder

  private def stripHtmlFromDiscoveryResponse(discoveryAsset: DiscoveryCollectionAsset) = {
    val resources = for {
      xsltStream <- Resource.make(IO(getClass.getResourceAsStream("/transform.xsl")))(is => IO(is.close()))
      inputStream <- Resource.make(IO(new ByteArrayInputStream(discoveryAsset.scopeContent.description.getBytes())))(is => IO(is.close()))
      outputStream <- Resource.make(IO(new ByteArrayOutputStream()))(bos => IO(bos.close()))
    } yield (xsltStream, inputStream, outputStream)
    resources.use {
      case (xsltStream, inputStream, outputStream) =>
        val factory = TransformerFactory.newInstance()
        val xslt = new StreamSource(xsltStream)
        val input = new StreamSource(inputStream)
        val result = new StreamResult(outputStream)
        val transformer = factory.newTransformer(xslt)
        transformer.transform(input, result)
        val newDescription = outputStream.toByteArray.map(_.toChar).mkString.trim
        val scopeContentWithNewDescription = discoveryAsset.scopeContent.copy(description = newDescription)
        val titleWithoutBackslashes = XML.loadString(discoveryAsset.title.replaceAll("\\\\", "")).text
        IO(discoveryAsset.copy(scopeContent = scopeContentWithNewDescription, title = titleWithoutBackslashes)).handleError(_ => discoveryAsset)
      case _ => IO(discoveryAsset)
    }
  }

  private def getAssetFromDiscoveryApi(citableReference: String): IO[DiscoveryCollectionAsset] = {
    val uri = uri"$discoveryBaseUrl/API/records/v1/collection/$citableReference"
    val request = basicRequest.get(uri).response(asJson[DiscoveryCollectionAssetResponse])
    for {
      response <- backend.send(request)
      body <- IO.fromEither(response.body)
      asset <- IO.fromOption(body.assets.find(_.citableReference == citableReference))(
        new Exception(s"Cannot find asset with citable reference $citableReference")
      )
      formattedAsset <- stripHtmlFromDiscoveryResponse(asset)
    } yield formattedAsset
  }

  def getDepartmentAndSeriesRows(input: Input): IO[DepartmentAndSeriesTableData] = {
    def tableEntry(asset: DiscoveryCollectionAsset) =
      DynamoTable(input.batchId, randomUuidGenerator(), "", asset.citableReference, folder, asset.title, asset.scopeContent.description)
    for {
      departmentDiscoveryAsset <- input.department.map(getAssetFromDiscoveryApi).sequence
      seriesDiscoveryAsset <- input.series.map(getAssetFromDiscoveryApi).sequence
    } yield {
      val departmentTableEntry = departmentDiscoveryAsset
        .map(tableEntry)
        .getOrElse(DynamoTable(input.batchId, randomUuidGenerator(), "", "Unknown", folder, "", ""))
      val seriesTableEntryOpt = seriesDiscoveryAsset
        .map(tableEntry)
        .map(_.copy(parentPath = departmentTableEntry.id.toString))
      DepartmentAndSeriesTableData(departmentTableEntry, seriesTableEntryOpt)
    }
  }
}
object DiscoveryService {
  case class DiscoveryScopeContent(description: String)
  case class DiscoveryCollectionAsset(citableReference: String, scopeContent: DiscoveryScopeContent, title: String)
  case class DiscoveryCollectionAssetResponse(assets: List[DiscoveryCollectionAsset])

  implicit val discoverScopeContentReader: Reader[DiscoveryScopeContent] = macroR[DiscoveryScopeContent]
  implicit val discoveryAssetReader: Reader[DiscoveryCollectionAsset] = macroR[DiscoveryCollectionAsset]
  implicit val discoveryResponseReader: Reader[DiscoveryCollectionAssetResponse] = macroR[DiscoveryCollectionAssetResponse]

  def apply(discoveryUrl: String, randomUuidGenerator: () => UUID): IO[DiscoveryService] = HttpClientFs2Backend.resource[IO]().use { backend =>
    IO(new DiscoveryService(discoveryUrl, backend, randomUuidGenerator))
  }
}
