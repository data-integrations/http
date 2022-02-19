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
package io.cdap.plugin.http.common.pagination.page;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.plugin.http.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.common.error.ErrorHandling;
import io.cdap.plugin.http.common.error.HttpErrorHandler;
import io.cdap.plugin.http.common.http.HttpResponse;

/**
 * Represents a page with an error
 */
class HttpErrorPage extends BasePage {
  private final BaseHttpSourceConfig config;
  private final HttpErrorHandler httpErrorHandler;
  private boolean isReturned = false;

  HttpErrorPage(BaseHttpSourceConfig config, HttpResponse httpResponse, HttpErrorHandler httpErrorHandler) {
    super(httpResponse);
    this.config = config;
    this.httpErrorHandler = httpErrorHandler;
  }

  @Override
  public String getPrimitiveByPath(String path) {
    // this should never happen, since the validation of configs is done during pipeline deployment
    throw new UnsupportedOperationException(String.format("Page format '%s' does not support searching by path",
                                                  config.getFormat()));
  }

  @Override
  public boolean hasNext() {
    return !isReturned;
  }

  @Override
  public PageEntry next() {
    isReturned = true;
    int httpCode = httpResponse.getStatusCode();
    // Body from blob might be a text for http status codes like 400 etc. That's why we still get body.
    // If it's not text though, it will only show up as undecodable gibberish.
    String body = httpResponse.getBody();

    InvalidEntry<StructuredRecord> invalidEntry = InvalidEntryCreator.buildStringError(
      httpCode, body, String.format("Request failed with '%d' http status code. Body is '%s'", httpCode, body));
    return new PageEntry(invalidEntry, getErrorHandlingStrategy());
  }

  @Override
  public void close() {

  }

  private ErrorHandling getErrorHandlingStrategy() {
    return httpErrorHandler
      .getErrorHandlingStrategy(httpResponse.getStatusCode())
      .getAfterRetryStrategy();
  }
}
