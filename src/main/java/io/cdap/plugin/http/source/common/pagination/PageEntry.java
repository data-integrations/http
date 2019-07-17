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
package io.cdap.plugin.http.source.common.pagination;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.InvalidEntry;

import java.util.Objects;

/**
 * A single text entry from a page. Ready to be converted to {@link StructuredRecord} or {@link InvalidEntry}.
 */
public class PageEntry {
  private final int httpCode;
  private final String body;

  public PageEntry(int httpCode, String body) {
    this.httpCode = httpCode;
    this.body = body;
  }

  public int getHttpCode() {
    return httpCode;
  }

  public String getBody() {
    return body;
  }

  @Override
  public boolean equals(Object o) {
    PageEntry pageEntry = (PageEntry) o;
    return httpCode == pageEntry.httpCode && Objects.equals(body, pageEntry.body);
  }

  @Override
  public int hashCode() {
    return Objects.hash(httpCode, body);
  }
}
