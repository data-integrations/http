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

import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.plugin.http.common.HttpErrorDetailsProvider;
import io.cdap.plugin.http.common.error.HttpErrorHandler;
import io.cdap.plugin.http.common.http.HttpResponse;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * A factory which creates instance of {@BasePage} in accordance to format configured in input config.
 * If erroneous page is being handled, {@HttpErrorPage} is returned, which returns a single error entry.
 */
public class PageFactory {
  public static BasePage createInstance(BaseHttpSourceConfig config, HttpResponse httpResponse,
                                        HttpErrorHandler httpErrorHandler, boolean isError) {
    if (isError) {
      return new HttpErrorPage(config, httpResponse, httpErrorHandler);
    }

    switch(config.getFormat()) {
      case JSON:
        try {
          return new JsonPage(config, httpResponse);
        } catch (Exception e) {
          throw getProgramFailureExceptionBasedOnStatusCode(httpResponse, "JSON", e);
        }
      case XML:
        try {
          return new XmlPage(config, httpResponse);
        } catch (Exception e) {
          throw getProgramFailureExceptionBasedOnStatusCode(httpResponse, "XML", e);
        }
      case TSV:
        try {
          return new DelimitedPage(config, httpResponse, "\t");
        } catch (IOException e) {
          throw getProgramFailureExceptionBasedOnStatusCode(httpResponse, "TSV", e);
        }
      case CSV:
        try {
          return new DelimitedPage(config, httpResponse, ",");
        } catch (IOException e) {
          throw getProgramFailureExceptionBasedOnStatusCode(httpResponse, "CSV", e);
        }
      case TEXT:
        try {
          return new TextPage(config, httpResponse);
        } catch (IOException e) {
          throw getProgramFailureExceptionBasedOnStatusCode(httpResponse, "TEXT", e);
        }
      case BLOB:
        return new BlobPage(config, httpResponse);
      default:
        throw new IllegalArgumentException(String.format("Unsupported page format: '%s'", config.getFormat()));
    }
  }

  private static ProgramFailureException getProgramFailureExceptionBasedOnStatusCode(
      HttpResponse httpResponse, String fileFormat, Exception e) {
    if (httpResponse.getStatusCode() != HttpURLConnection.HTTP_OK) {
      ErrorUtils.ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(
          httpResponse.getStatusCode());
      String errorReason = String.format(
          "Failed to read %s page with status code: %s. %s. For more details, see %s", fileFormat,
          httpResponse.getStatusCode(), pair.getCorrectiveAction(),
          HttpErrorDetailsProvider.getSupportedDocumentUrl());
      String errorMessage = String.format(
          "Failed to read %s page with status code: %s, message: %s", fileFormat,
          httpResponse.getStatusCode(), e.getMessage());
      return ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason, errorMessage,
          pair.getErrorType(), true, ErrorCodeType.HTTP,
          String.valueOf(httpResponse.getStatusCode()),
          HttpErrorDetailsProvider.getSupportedDocumentUrl(), e);
    } else {
      String errorReason = String.format("Failed to read %s page, %s: %s", fileFormat,
          e.getClass().getName(), e.getMessage());
      return ErrorUtils.getProgramFailureException(
          new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason, errorReason,
          ErrorType.SYSTEM, true, e);
    }
  }
}
