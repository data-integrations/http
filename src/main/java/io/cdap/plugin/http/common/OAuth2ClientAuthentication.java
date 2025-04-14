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
 * Enum encoding the handled Oauth2 Client Authentication
 */
public enum OAuth2ClientAuthentication implements EnumWithValue {
  BODY("body", "Body"),
  REQUEST_PARAMETER("request_parameter", "Request Parameter"),
  BASIC_AUTH_HEADER("basic_auth_header", "Basic Auth Header");

  private final String value;
  private final String label;

  OAuth2ClientAuthentication(String value, String label) {
    this.value = value;
    this.label = label;
  }

  /**
   * Determines the OAuth2 client authentication method based on the provided input.
   *
   * <p>This method checks if the given client authentication type matches the predefined
   *  authentication type. If it matches, the method returns the same authentication. Otherwise,
   * it defaults to BASIC_AUTH_HEADER authentication.</p>
   *
   * @param clientAuthentication The client authentication type as a {@link String}. It can be
   *                             either the value or the label of the authentication method.
   * @return {@link OAuth2ClientAuthentication} The corresponding authentication type.
   */
  public static OAuth2ClientAuthentication getClientAuthentication(String clientAuthentication) {
    if (Objects.equals(clientAuthentication, BODY.getValue()) || Objects.equals(
        clientAuthentication, BODY.getLabel())) {
      return BODY;
    } else if (Objects.equals(clientAuthentication, REQUEST_PARAMETER.getValue()) || Objects.equals(
      clientAuthentication, REQUEST_PARAMETER.getLabel())) {
      return REQUEST_PARAMETER;
    } else {
      return BASIC_AUTH_HEADER;
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
