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

import com.google.common.base.Splitter;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.format.delimited.common.DelimitedStructuredRecordStringConverter;
import io.cdap.plugin.format.delimited.input.SplitQuotesIterator;
import io.cdap.plugin.http.common.http.HttpResponse;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;


import java.io.IOException;
import java.util.Iterator;
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
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    Iterator<String> splitsIterator = getSplitsIterator(config.getEnableQuotesValues(), line, delimiter);
    Iterator<Schema.Field> fields = schema.getFields().iterator();
    while (splitsIterator.hasNext()) {
      Schema.Field field = fields.next();
      DelimitedStructuredRecordStringConverter.parseAndSetFieldValue(builder, field, splitsIterator.next());
    }
    return builder.build();
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

  private Iterator<String> getSplitsIterator(boolean enableQuotesValue, String delimitedString, String delimiter) {
    if (enableQuotesValue) {
      return new SplitQuotesIterator(delimitedString, delimiter, null, false);
    } else {
      return Splitter.on(delimiter).split(delimitedString).iterator();
    }
  }
}
