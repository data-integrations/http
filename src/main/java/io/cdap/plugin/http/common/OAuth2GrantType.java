/*
 * Copyright © 2025 Cask Data, Inc.
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

package io.cdap.plugin.http.common;

import java.util.Objects;

/**
 * Enum encoding the handled Oauth2 Grant Types
 */
public enum OAuth2GrantType implements EnumWithValue {
  REFRESH_TOKEN("refresh_token", "Refresh Token"),
  CLIENT_CREDENTIALS("client_credentials", "Client Credentials");

  private final String value;
  private final String label;

  OAuth2GrantType(String value, String label) {
    this.value = value;
    this.label = label;
  }

  /**
   * Determines the OAuth2 grant type based on the provided string value.
   *
   * <p>This method checks whether the given OAuth2 grant type string matches
   * the CLIENT_CREDENTIALS grant type based on its value or label. If it matches,
   * CLIENT_CREDENTIALS is returned; otherwise, REFRESH_TOKEN is returned as the default.</p>
   *
   * @param oauth2GrantType The OAuth2 grant type as a string.
   * @return The corresponding {@link OAuth2GrantType}, either CLIENT_CREDENTIALS or REFRESH_TOKEN.
   */
  public static OAuth2GrantType getGrantType(String oauth2GrantType) {
    if (Objects.equals(oauth2GrantType, CLIENT_CREDENTIALS.getValue()) || Objects.equals(
        oauth2GrantType, CLIENT_CREDENTIALS.getLabel())) {
      return CLIENT_CREDENTIALS;
    } else {
      return REFRESH_TOKEN;
    }
  }
  
  @Override
  public String getValue() {
    return value;
  }

  public String getLabel() {
    return label;
  }

  @Override
  public String toString() {
    return this.getValue();
  }
}
