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
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.http.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.common.http.HttpResponse;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Converts page lines of tsv and csv into structured records.
 */
public class DelimitedPage extends RecordPerLinePage {
  private final String delimiter;
  private boolean isFirstRowSkipped;

  DelimitedPage(BaseHttpSourceConfig config, HttpResponse httpResponse, String delimiter) throws IOException {
    super(config, httpResponse);
    this.delimiter = delimiter;
    this.isFirstRowSkipped = false;
  }

  @Override
  protected StructuredRecord getStructedRecordByString(String line) {
    return StructuredRecordStringConverter.fromDelimitedString(line, delimiter, schema);
  }

  @Override
  public PageEntry next() {
    if (this.config.getCsvSkipFirstRow() && !isFirstRowSkipped) {
      isFirstRowSkipped = true;
      if (!hasNext()) { // calling hasNext will also read the line;
        throw new NoSuchElementException();
      }
      isLineRead = false;
    }
    return super.next();
  }
}
