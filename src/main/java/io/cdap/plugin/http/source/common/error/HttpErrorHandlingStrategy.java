/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.http.source.common.error;

import io.cdap.plugin.http.source.common.EnumWithValue;

/**
 * Indicates error handling strategy which will be used to handle unexpected http status codes.
 */
public enum HttpErrorHandlingStrategy implements EnumWithValue {
  SUCCESS("Success"),
  FAIL("Fail"),
  SKIP("Skip"),
  SEND_TO_ERROR("Send to error"),
  RETRY_AND_FAIL("Retry and fail"),
  RETRY_AND_SKIP("Retry and skip"),
  RETRY_AND_SEND_TO_ERROR("Retry and send to error");

  private final String value;

  HttpErrorHandlingStrategy(String value) {
    this.value = value;
  }

  @Override
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return this.getValue();
  }

  public boolean shouldRetry() {
    return (this.equals(RETRY_AND_FAIL) || this.equals(RETRY_AND_SKIP) || this.equals(RETRY_AND_SEND_TO_ERROR));
  }

  public HttpErrorHandlingStrategy getAfterRetryStrategy() {
    switch (this) {
      case RETRY_AND_FAIL:
        return FAIL;
      case RETRY_AND_SKIP:
        return SKIP;
      case RETRY_AND_SEND_TO_ERROR:
        return SEND_TO_ERROR;
      default:
        return this;
    }
  }
}
