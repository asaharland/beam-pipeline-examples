package com.harland.example.common.options;

import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface BigQueryImportOptions extends PipelineOptions, S3Options {

  @Description("BigQuery Table Name")
  @Default.String("my-project:my_dataset.my_table")
  String getBqTableName();

  void setBqTableName(String tableName);

  @Description("Import location in the format gs://<BUCKET_NAME> or s3://<BUCKET_NAME>")
  @Default.String("gs://my-bucket")
  String getBucketUrl();

  void setBucketUrl(String bucketUrl);

  @Description("AWS S3 Key ID")
  @Default.String("KEY")
  String getAwsAccessKey();

  void setAwsAccessKey(String awsAccessKey);

  @Description("AWS S3 Secret Key")
  @Default.String("SECRET KEY")
  String getAwsSecretKey();

  void setAwsSecretKey(String awsSecretKey);
}
