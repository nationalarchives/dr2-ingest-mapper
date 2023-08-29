# DR2 Ingest Mapper

This lambda reads a bagit package based on the input to the lambda, parses file metadata and writes this to a DynamoDB table.

The lambda:
* Reads the input from the step function step with this format:
```json
{
  "batchId": "batch",
  "s3Bucket": "bucket",
  "s3Prefix": "prefix/",
  "department": "departmnet",
  "series": "series"
}
```
* Gets the title and description for department and series from discovery. This is run through the XSLT in `src/main/resources/transform.xsl` to replace the EAD tags with newlines.
* Parses the folder metadata csv
* Parses the asset metadata csv
* Parses the file metadata csv
* Parses the manifest file
* Converts these into Dynamo case classes
```scala
  case class DynamoTable(
      batchId: String,
      id: UUID,
      parentPath: String,
      name: String,
      `type`: String,
      title: String,
      description: String,
      fileSize: Option[Long] = None,
      checksumSha256: Option[String] = None,
      fileExtension: Option[String] = None
  )
```
* Updates dynamo with the values
* Writes the state data for the next step function step with this format:
```json
{
  "batchId": "TDR-2023-ABC",
  "rootPath": "s3://<env>-dr2-ingest-raw-cache/TDR-2023-ABC/",
  "batchType": "courtDocument",
  "archiveHierarchyFolders": [
      "f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176",
      "e88e433a-1f3e-48c5-b15f-234c0e663c27",
      "93f5a200-9ee7-423d-827c-aad823182ad2"
  ],
  "contentFolders": [],
  "contentAssets": [
      "a8163bde-7daa-43a7-9363-644f93fe2f2b"
  ]
}
```



[Link to the infrastructure code](https://github.com/nationalarchives/dp-terraform-environments/blob/main/ingest_mapper.tf)

## Environment Variables

| Name              | Description                      |
|-------------------|----------------------------------|
| DYNAMO_TABLE_NAME | The table to write the values to |
