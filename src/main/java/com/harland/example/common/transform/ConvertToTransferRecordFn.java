package com.harland.example.common.transform;

import com.harland.example.common.model.TransferRecord;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Takes a String representing comma delimited row and converts it to a BigQuery TableRow for
 * storage.
 */
public class ConvertToTransferRecordFn extends DoFn<String, TransferRecord> {

  @ProcessElement
  public void processElement(@Element String row, OutputReceiver<TransferRecord> receiver) {
    String[] fields = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

    // Skips over the header row
    if (!fields[0].equals("user_id")) {
      receiver.output(new TransferRecord(fields[0], fields[1], Double.parseDouble(fields[2])));
    }
  }
}
