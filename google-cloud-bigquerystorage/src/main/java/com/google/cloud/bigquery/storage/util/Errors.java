package com.google.cloud.bigquery.storage.util;

import io.grpc.Status;

/**
 * Static utility methods for working with Errors returned from the service.
 */
public class Errors {
   private Errors(){};

  /**
   * Returns true iff the Status indicates and internal error that is retryable.
   *
   * Generally, internal errors are not considered retryable, however there are certain transient
   * network issues that appear as internal but are in fact retryable.
   */
  public static boolean isRetryableInternalStatus(Status status) {
    return status.getCode() == Status.Code.INTERNAL
        && status.getDescription() != null
        && (status.getDescription().contains("Received unexpected EOS on DATA frame from server")
        || status.getDescription().contains("Received Rst Stream")
        || status.getCause().contains("RST_STREAM"));
  }

}
