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
package io.cdap.plugin.http.common.pagination;

import io.cdap.plugin.http.common.EnumWithValue;

/**
 * An enum which represent a type of pagination.
 */
public enum PaginationType implements EnumWithValue {

  NONE("None"),

  LINK_IN_RESPONSE_HEADER("Link in response header"),

  LINK_IN_RESPONSE_BODY("Link in response body"),

  TOKEN_IN_RESPONSE_BODY("Token in response body"),

  INCREMENT_AN_INDEX("Increment an index"),

  CUSTOM("Custom");

  private final String value;

  PaginationType(String value) {
    this.value = value;
  }

  @Override
  public String getValue() {
    return value;
  }

  @Override
  public String toString() {
    return this.getValue();
  }
}
