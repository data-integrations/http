/*
 * Copyright © 2024 Cask Data, Inc.
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
package io.cdap.plugin.http.source.common;


import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.plugin.http.common.HttpErrorDetailsProvider;
import io.cdap.plugin.http.common.http.HttpResponse;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.util.Iterator;

/**
 * Class that reads the raw string from the HTTP response and returns it line by line.
 */
public class RawStringPerLine implements Closeable, Iterator<String> {
    protected final HttpResponse httpResponse;
    private BufferedReader bufferedReader;
    private boolean isLineRead;
    private String lastLine;

    public RawStringPerLine(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

    private BufferedReader getBufferedReader()  {
        if (bufferedReader == null) {
          try {
            this.bufferedReader = new BufferedReader(new InputStreamReader(httpResponse.getInputStream()));
          } catch (IOException e) {
            String errorMessage = "Unable to create a buffered reader for the http response.";
            throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
              errorMessage, errorMessage, ErrorType.SYSTEM, true, null);
          }
        }
        return bufferedReader;
    }

    @Override
    public void close() {
        if (bufferedReader != null) {
          try {
            bufferedReader.close();
          } catch (IOException e) {
              String errorReason = "Unable to close the buffered reader for the http response.";
              String errorMessage = String.format(
                  "Unable to close the buffered reader for the http response, %s: %s",
                  e.getClass().getName(), e.getMessage());
              throw ErrorUtils.getProgramFailureException(
                  new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason,
                  errorMessage, ErrorType.SYSTEM, false, e);
          }
        }
    }

    @Override
    public boolean hasNext() {
        try {
            if (!isLineRead) {
                lastLine = this.getBufferedReader().readLine();
            }
            isLineRead = true;
            return lastLine != null;
        } catch (IOException e) { // we need to catch this, since hasNext() does not have "throws" in parent
            if (httpResponse.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorUtils.ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(
                    httpResponse.getStatusCode());
                String errorReason = String.format(
                    "Unable to read line from http page buffer: %s. %s. For more details, see %s",
                    httpResponse.getStatusCode(), pair.getCorrectiveAction(),
                    HttpErrorDetailsProvider.getSupportedDocumentUrl());
                String errorMessage = String.format(
                    "Unable to read line from http page buffer with code: %s, message: %s",
                    httpResponse.getStatusCode(), e.getMessage());
                throw ErrorUtils.getProgramFailureException(
                    new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason,
                    errorMessage, pair.getErrorType(), true, ErrorCodeType.HTTP,
                    String.valueOf(httpResponse.getStatusCode()),
                    HttpErrorDetailsProvider.getSupportedDocumentUrl(), e);
            } else {
                String errorReason = String.format(
                    "Unable to read line from http page buffer with message: %s", e.getMessage());
                throw ErrorUtils.getProgramFailureException(
                    new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason,
                    errorReason, ErrorType.SYSTEM, true, e);
            }
        }
    }

    @Override
    public String next() {
        if (!hasNext()) { // calling hasNext will also read the line;
            if (httpResponse.getStatusCode() != HttpURLConnection.HTTP_OK) {
                ErrorUtils.ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(httpResponse.getStatusCode());
                String errorReason = String.format(
                    "Failed to read the next line with error code: %s. %s. For more details, see %s",
                    httpResponse.getStatusCode(), pair.getCorrectiveAction(),
                    HttpErrorDetailsProvider.getSupportedDocumentUrl());
                String errorMessage = String.format(
                    "Failed to read the next line with error code: %s. %s",
                    httpResponse.getStatusCode(), pair.getCorrectiveAction());
                throw ErrorUtils.getProgramFailureException(
                    new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason,
                    errorMessage, pair.getErrorType(), true, ErrorCodeType.HTTP,
                    String.valueOf(httpResponse.getStatusCode()),
                    HttpErrorDetailsProvider.getSupportedDocumentUrl(), null);
            } else {
                String errorReason = "Failed to read the next line.";
                throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
                  errorReason, errorReason, ErrorType.SYSTEM, true, null);
            }
        }
        isLineRead = false;
        return lastLine;
    }
}
