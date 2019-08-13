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
package io.cdap.plugin.http.source.common.pagination.page;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.http.HttpResponse;

import java.io.IOException;

/**
 * Returns text as structured records with a single bytes field.
 */
class BlobPage extends BasePage {
  private final Schema schema;
  private final BaseHttpSourceConfig config;
  private boolean isReturned = false;

  BlobPage(BaseHttpSourceConfig config, HttpResponse httpResponse) {
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

  @Override
  public boolean hasNext() {
    return !isReturned;
  }

  @Override
  public PageEntry next() {
    byte[] bytes = httpResponse.getBytes();
    try {
      StructuredRecord.Builder builder = StructuredRecord.builder(schema);
      builder.set(schema.getFields().get(0).getName(), bytes);
      isReturned = true;
      return new PageEntry(builder.build());
    } catch (Throwable e) {
      return new PageEntry(InvalidEntryCreator.buildBytesError(bytes, e), config.getErrorHandling());
    }
  }

  @Override
  public void close() throws IOException {

  }
}
