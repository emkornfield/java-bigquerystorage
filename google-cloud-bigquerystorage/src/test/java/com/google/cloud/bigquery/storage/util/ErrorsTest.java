package com.google.cloud.bigquery.storage.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.grpc.Status;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ErrorsTest {

  @Test
  public void testRetryableInternalForRstErrors() {
    assertTrue(Errors.isRetryableInternalStatus(Status.INTERNAL.withDescription("HTTP/2 error code: INTERNAL_ERROR\nReceived Rst stream")));
    assertTrue(Errors.isRetryableInternalStatus(Status.INTERNAL.withDescription("RST_STREAM closed stream. HTTP/2 error code: INTERNAL_ERROR")));
  }

  @Test
  public void testNonRetryableInternalError() {
    assertFalse(Errors.isRetryableInternalStatus(Status.INTERNAL));
    assertFalse(Errors.isRetryableInternalStatus(Status.INTERNAL.withDescription("Server error.")));
  }

  @Test
  public void testNonRetryableOtherError() {
    assertTrue(Errors.isRetryableInternalStatus(Status.DATA_LOSS.withDescription("RST_STREAM closed stream. HTTP/2 error code: INTERNAL_ERROR")));
  }
}
