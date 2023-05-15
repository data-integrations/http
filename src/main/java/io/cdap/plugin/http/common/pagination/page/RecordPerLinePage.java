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
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.http.common.http.HttpResponse;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.NoSuchElementException;

/**
 * Returns every row of document as a structured record.
 */
abstract class RecordPerLinePage extends BasePage {
  private BufferedReader bufferedReader;
  protected final Schema schema;
  protected final BaseHttpSourceConfig config;
  protected boolean isLineRead;
  private String lastLine;

  RecordPerLinePage(BaseHttpSourceConfig config, HttpResponse httpResponse) {
    super(httpResponse);
    this.config = config;
    this.schema = config.getSchema();
  }

  @Override
  public String getPrimitiveByPath(String path) {
    // this should never happen, since the validation of configs is done during pipeline deployment
    throw new UnsupportedOperationException(String.format("Page format '%s' does not support searching by path",
                                                          config.getFormat()));
  }

  private BufferedReader getBufferedReader() throws IOException {
    if (bufferedReader == null) {
      this.bufferedReader = new BufferedReader(new InputStreamReader(httpResponse.getInputStream()));
    }
    return bufferedReader;
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
      throw new RuntimeException("Failed to read line from http page buffer", e);
    }
  }

  @Override
  public PageEntry next() {
    if (!hasNext()) { // calling hasNext will also read the line;
      throw new NoSuchElementException();
    }
    isLineRead = false;
    try {
      return new PageEntry(getStructedRecordByString(lastLine));
    } catch (Throwable e) {
      return new PageEntry(InvalidEntryCreator.buildStringError(lastLine, e), config.getErrorHandling());
    }
  }

  @Override
  public void close() throws IOException {
    if (this.bufferedReader != null) {
      this.bufferedReader.close();
    }
  }

  protected abstract StructuredRecord getStructedRecordByString(String line);
}
