/*
 * Copyright © 2017 Cask Data, Inc.
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

/**
 * HTTP Plugin Constants
 */
public final class HttpConstants {
  private HttpConstants() {

  }

  public static final String HTTP_PLUGIN_NAME = "HTTP";

  // Field Name Constants
  public static final String PROPERTY_URL = "url";
  public static final String PROPERTY_HTTP_METHOD = "httpMethod";
  public static final String PROPERTY_HEADERS = "headers";
  public static final String PROPERTY_REQUEST_BODY = "requestBody";
  public static final String PROPERTY_USERNAME = "username";
  public static final String PROPERTY_PASSWORD = "password";
  public static final String PROPERTY_PROXY_URL = "proxyUrl";
  public static final String PROPERTY_PROXY_USERNAME = "proxyUsername";
  public static final String PROPERTY_PROXY_PASSWORD = "proxyPassword";
  public static final String PROPERTY_HTTP_ERROR_HANDLING = "httpErrorsHandling";
  public static final String PROPERTY_ERROR_HANDLING = "errorHandling";
  public static final String PROPERTY_RETRY_POLICY = "retryPolicy";
  public static final String PROPERTY_CONNECT_TIMEOUT = "connectTimeout";
  public static final String PROPERTY_READ_TIMEOUT = "readTimeout";
  public static final String PROPERTY_OAUTH2_ENABLED = "oauth2Enabled";
  public static final String PROPERTY_AUTH_URL = "authUrl";
  public static final String PROPERTY_TOKEN_URL = "tokenUrl";
  public static final String PROPERTY_CLIENT_ID = "clientId";
  public static final String PROPERTY_CLIENT_SECRET = "clientSecret";
  public static final String PROPERTY_SCOPES = "scopes";
  public static final String PROPERTY_REFRESH_TOKEN = "refreshToken";
  public static final String PROPERTY_VERIFY_HTTPS = "verifyHttps";
  public static final String PROPERTY_KEYSTORE_FILE = "keystoreFile";
  public static final String PROPERTY_KEYSTORE_TYPE = "keystoreType";
  public static final String PROPERTY_KEYSTORE_PASSWORD = "keystorePassword";
  public static final String PROPERTY_KEYSTORE_KEY_ALGORITHM = "keystoreKeyAlgorithm";
  public static final String PROPERTY_TRUSTSTORE_FILE = "trustStoreFile";
  public static final String PROPERTY_TRUSTSTORE_TYPE = "trustStoreType";
  public static final String PROPERTY_TRUSTSTORE_PASSWORD = "trustStorePassword";
  public static final String PROPERTY_TRUSTSTORE_KEY_ALGORITHM = "trustStoreKeyAlgorithm";
  public static final String PROPERTY_TRANSPORT_PROTOCOLS = "transportProtocols";
  public static final String PROPERTY_CIPHER_SUITES = "cipherSuites";
  public static final String PROPERTY_SCHEMA = "schema";
  public static final String PROPERTY_KEYSTORE_CERT_ALIAS = "keystoreCertAlias";
}
