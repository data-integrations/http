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
import io.cdap.plugin.http.common.error.ErrorHandling;

/**
 * Represents a single entry found on page. The entry can either be an {@link InvalidEntry} or {@link StructuredRecord}.
 */
public class PageEntry {
  private final StructuredRecord record;
  private final InvalidEntry<StructuredRecord> error;
  private final ErrorHandling errorHandling;

  public PageEntry(StructuredRecord record) {
    this.record = record;
    this.error = null;
    this.errorHandling = null;
  }

  public PageEntry(InvalidEntry<StructuredRecord> error, ErrorHandling errorHandling) {
    this.record = null;
    this.error = error;
    this.errorHandling = errorHandling;
  }

  public boolean isError() {
    return error != null;
  }

  public StructuredRecord getRecord() {
    return record;
  }

  public InvalidEntry<StructuredRecord> getError() {
    return error;
  }

  public ErrorHandling getErrorHandling() {
    return errorHandling;
  }
}
