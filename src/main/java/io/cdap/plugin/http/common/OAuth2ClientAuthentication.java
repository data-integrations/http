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
  REQUEST_PARAMETER("request_parameter", "Request Parameter");

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
   * BODY authentication type. If it matches, the method returns the BODY authentication. Otherwise,
   * it defaults to REQUEST_PARAMETER authentication.</p>
   *
   * @param clientAuthentication The client authentication type as a {@link String}. It can be
   *                             either the value or the label of the BODY authentication method.
   * @return {@link OAuth2ClientAuthentication} The corresponding authentication type. Returns
   * {@code BODY} if the input matches its value or label; otherwise, returns
   * {@code REQUEST_PARAMETER}.
   */
  public static OAuth2ClientAuthentication getClientAuthentication(String clientAuthentication) {
    if (Objects.equals(clientAuthentication, BODY.getValue()) || Objects.equals(
        clientAuthentication, BODY.getLabel())) {
      return BODY;
    } else {
      return REQUEST_PARAMETER;
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
