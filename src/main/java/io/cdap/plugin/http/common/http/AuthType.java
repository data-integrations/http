/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.http.common.http;


import io.cdap.plugin.http.common.exceptions.InvalidPropertyTypeException;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Enum for different authentication methods.
 */
public enum AuthType {
  NONE("none"),
  OAUTH2("oAuth2"),
  SERVICE_ACCOUNT("serviceAccount"),
  BASIC_AUTH("basicAuth");

  private final String value;

  AuthType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  /**
   * Returns the AuthType.
   *
   * @param value the value is string type.
   * @return The AuthType
   */
  public static AuthType fromValue(String value) {
    return Arrays.stream(AuthType.values()).filter(authtype -> authtype.getValue().equals(value))
      .findAny().orElseThrow(() -> new InvalidPropertyTypeException(BaseHttpSourceConfig.PROPERTY_AUTH_TYPE_LABEL,
        value, getAllowedValues()));
  }

  public static List<String> getAllowedValues() {
    return Arrays.stream(AuthType.values()).map(v -> v.getValue())
      .collect(Collectors.toList());
  }
}

