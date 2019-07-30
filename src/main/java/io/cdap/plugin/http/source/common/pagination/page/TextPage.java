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
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.http.HttpResponse;

import java.io.IOException;

/**
 * Iterates over every line of text page and returns them as structured records with a single string field.
 */
public class TextPage extends RecordPerLinePage {

  TextPage(BaseHttpSourceConfig config, HttpResponse httpResponse) throws IOException {
    super(config, httpResponse);
  }

  @Override
  protected StructuredRecord getStructedRecordByString(String line) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    builder.set(schema.getFields().get(0).getName(), line);
    return builder.build();
  }
}
