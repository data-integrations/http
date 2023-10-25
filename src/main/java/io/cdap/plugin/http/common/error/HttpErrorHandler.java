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
package io.cdap.plugin.http.common.error;

import io.cdap.plugin.http.sink.batch.HTTPSinkConfig;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Matcher;

/**
 * A class which calculates an error handling strategy for http status codes.
 */
public class HttpErrorHandler {
  private static final Logger LOG = LoggerFactory.getLogger(HttpErrorHandler.class);

  private List<HttpErrorHandlerEntity> httpErrorsHandlingEntries;

  public HttpErrorHandler(BaseHttpSourceConfig config) {
    this.httpErrorsHandlingEntries = config.getHttpErrorHandlingEntries();
  }

  public HttpErrorHandler(HTTPSinkConfig config) {
    this.httpErrorsHandlingEntries = config.getHttpErrorHandlingEntries();
  }

  public RetryableErrorHandling getErrorHandlingStrategy(int httpCode) {
    String httpCodeString = Integer.toString(httpCode);

    for (HttpErrorHandlerEntity httpErrorsHandlingEntry : httpErrorsHandlingEntries) {
      Matcher matcher = httpErrorsHandlingEntry.getPattern().matcher(httpCodeString);
      if (matcher.matches()) {
        return httpErrorsHandlingEntry.getStrategy();
      }
    }

    LOG.warn(String.format("No error handling strategy defined for HTTP status code '%d'. " +
                             "Please correct httpErrorsHandling.", httpCode));
    return RetryableErrorHandling.FAIL;
  }
}
