package com.harland.example.streaming.options;

import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface StreamingFilePipelineOptions extends PipelineOptions, S3Options {

  @Description("BigQuery Table Name")
  @Default.String("my-project:my_dataset.my_table")
  String getBqTableName();

  void setBqTableName(String tableName);

  @Description("Import location in the format gs://<BUCKET_NAME> or s3://<BUCKET_NAME>")
  @Default.String("gs://my-bucket")
  String getBucketUrl();

  void setBucketUrl(String bucketUrl);
}
