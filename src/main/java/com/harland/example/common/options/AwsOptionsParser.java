package com.harland.example.common.options;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

public class AwsOptionsParser {

  // AWS configuration values
  private static final String AWS_DEFAULT_REGION = "eu-west-1";
  private static final String AWS_S3_PREFIX = "s3";

  /**
   * Formats BigQueryImportOptions to include AWS specific configuration.
   *
   * @param options for running the Cloud Dataflow pipeline.
   */
  public static void formatOptions(BigQueryImportOptions options) {
    if (options.getBucketUrl().toLowerCase().startsWith(AWS_S3_PREFIX)) {
      setAwsCredentials(options);
    }

    if (options.getAwsRegion() == null) {
      setAwsDefaultRegion(options);
    }
  }

  private static void setAwsCredentials(BigQueryImportOptions options) {
    options.setAwsCredentialsProvider(
        new AWSStaticCredentialsProvider(
            new BasicAWSCredentials(options.getAwsAccessKey(), options.getAwsSecretKey())));
  }

  private static void setAwsDefaultRegion(BigQueryImportOptions options) {
    if (options.getAwsRegion() == null) {
      options.setAwsRegion(AWS_DEFAULT_REGION);
    }
  }
}
