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
package io.cdap.plugin.http.common.http;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import io.cdap.plugin.http.common.BaseHttpConfig;
import io.cdap.plugin.http.common.OAuth2ClientAuthentication;
import io.cdap.plugin.http.common.OAuth2GrantType;
import io.cdap.plugin.http.common.pagination.page.JSONUtil;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A class which contains utilities to make OAuth2 specific calls.
 */
public class OAuthUtil {

  /**
   * Get Authorization header based on the config parameters provided
   *
   * @param config
   * @return
   * @throws IOException while creating the AccessToken
   */
  @Nullable
  public static AccessToken getAccessToken(BaseHttpConfig config) throws IOException {

    // auth check
    AuthType authType = config.getAuthType();

    // backward compatibility
    if (config.getOauth2Enabled()) {
      authType = AuthType.OAUTH2;
    }

    switch (authType) {
      case SERVICE_ACCOUNT:
        // get accessToken from service account
        return OAuthUtil.getAccessTokenByServiceAccount(config);
      case OAUTH2:
        try (CloseableHttpClient client = HttpClients.createDefault()) {
          return getAccessToken(client, config);
        }
    }
    return null;
  }

  /**
   * Retrieves an OAuth 2.0 access token based on the specified grant type.
   *
   * <p>This method supports obtaining an access token using either the {@code REFRESH_TOKEN}
   * or {@code CLIENT_CREDENTIALS} grant type. If an invalid grant type is provided, an
   * {@link IOException} is thrown.</p>
   *
   * @param httpclient the {@link CloseableHttpClient} instance used to execute HTTP requests.
   * @param config     the {@link BaseHttpConfig} instance containing OAuth 2.0 configuration
   *                   details.
   * @return an {@link AccessToken} object containing the retrieved access token.
   * @throws IOException if an error occurs during the HTTP request or if the grant type is
   *                     invalid.
   */
  public static AccessToken getAccessToken(CloseableHttpClient httpclient, BaseHttpConfig config)
      throws IOException {
    switch (config.getOauth2GrantType()) {
      case REFRESH_TOKEN:
        return getAccessTokenByRefreshToken(httpclient, config);
      case CLIENT_CREDENTIALS:
        return getAccessTokenByClientCredentials(httpclient, config);
      default:
        throw new IllegalArgumentException(
            String.format("Invalid Grant Type: %s. Cannot retrieve access token.",
                config.getOauth2GrantType()));
    }
  }

  /**
   * Retrieves an OAuth2 access token using the Client Credentials grant type.
   *
   * <p>This method constructs an HTTP POST request to fetch an access token from the authorization
   * server. The client authentication method (either "BODY" or "REQUEST") determines whether client
   * credentials are sent in the request body or as query parameters in the URL.</p>
   *
   * <p>Steps:
   * 1. If client authentication is set to "BODY": - Constructs a URI using the token URL. - Adds
   * necessary parameters (scope, grant_type, client_id, client_secret) in the request body. -
   * Creates an HTTP POST request and sets the entity with encoded parameters.
   * <br>
   * 2. If client authentication is set to "REQUEST": - Constructs a URI with client credentials as
   * query parameters. - Creates an HTTP POST request with the URI.
   * <br>
   * 3. Calls `fetchAccessToken(httpclient,httppost)` to execute the request and retrieve the
   * token.
   *
   * @param httpclient           The HTTP client to execute the request.
   * @return An AccessToken object containing the token and expiration details.
   * @throws IOException              If an error occurs while executing the request.
   * @throws IllegalArgumentException If the token URL cannot be built properly.
   */
  public static AccessToken getAccessTokenByClientCredentials(CloseableHttpClient httpclient,
      BaseHttpConfig config) throws IOException {
    URI uri;
    HttpPost httppost;
    try {
      if (Objects.equals(config.getOauth2ClientAuthentication().getValue(),
          OAuth2ClientAuthentication.BODY.getValue())) {
        uri = new URIBuilder(config.getTokenUrl()).build();
        List<BasicNameValuePair> nameValuePairs = new ArrayList<>();
        nameValuePairs.add(
            new BasicNameValuePair("grant_type", OAuth2GrantType.CLIENT_CREDENTIALS.getValue()));
        nameValuePairs.add(new BasicNameValuePair("client_id", config.getClientId()));
        nameValuePairs.add(new BasicNameValuePair("client_secret", config.getClientSecret()));
        if (!Strings.isNullOrEmpty(config.getScopes())) {
          nameValuePairs.add(new BasicNameValuePair("scope", config.getScopes()));
        }
        httppost = new HttpPost(uri);
        httppost.setEntity(new UrlEncodedFormEntity(nameValuePairs));
      } else {
        URIBuilder uriBuilder = new URIBuilder(config.getTokenUrl()).setParameter("client_id",
                config.getClientId()).setParameter("client_secret", config.getClientSecret())
            .setParameter("grant_type", OAuth2GrantType.CLIENT_CREDENTIALS.getValue());
        if (!Strings.isNullOrEmpty(config.getScopes())) {
          uriBuilder.setParameter("scope", config.getScopes());
        }
        uri = uriBuilder.build();
        httppost = new HttpPost(uri);
      }
      return fetchAccessToken(httpclient, httppost);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Failed to build access token URI for OAuth2 with grant type = "
              + OAuth2GrantType.CLIENT_CREDENTIALS.getValue(), e);
    }
  }

  /**
   * Returns true only if the expiration time set in the accessToken is before the current time.
   * @param accessToken AccessToken instance
   * @return  TRUE    if expiration time < current system time
   *          FALSE   if the accessToken is null
   *          FALSE   if the accessToken does not contain an expirationTime
   */
  public static boolean tokenExpired(AccessToken accessToken) {
    if (accessToken != null) {
      Date expiryTime = accessToken.getExpirationTime();
      if (expiryTime != null) {
        if (Date.from(Instant.now()).after(expiryTime)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Get the Access Token using the Refresh Token. The AccessToken returned has a valid expiration time if the
   * token URL used to create the token returned a valid expires_in detail in the response.
   *
   * @param httpclient
   * @param config
   * @return
   * @throws IOException
   */
  public static AccessToken getAccessTokenByRefreshToken(CloseableHttpClient httpclient,
                                                         BaseHttpConfig config) throws IOException {
    URI uri;
    try {
      uri = new URIBuilder(config.getTokenUrl())
              .setParameter("client_id", config.getClientId())
              .setParameter("client_secret", config.getClientSecret())
              .setParameter("refresh_token", config.getRefreshToken())
              .setParameter("grant_type", "refresh_token")
              .build();
      HttpPost httppost = new HttpPost(uri);
      return fetchAccessToken(httpclient, httppost);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to build token URI for OAuth2", e);
    }
  }

  /**
   * Fetches an OAuth2 access token by executing an HTTP POST request.
   *
   * @param httpclient The HTTP client used to execute the request.
   * @param httppost   The HTTP POST request containing the authentication details.
   * @return An AccessToken object containing the token string and expiration date.
   * @throws IOException If an error occurs while executing the request or processing the response.
   */
  private static AccessToken fetchAccessToken(CloseableHttpClient httpclient, HttpPost httppost)
      throws IOException {
    CloseableHttpResponse response = httpclient.execute(httppost);
    String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");

    JsonElement accessTokenElement = JSONUtil.toJsonObject(responseString).get("access_token");
    if (accessTokenElement == null) {
      throw new IllegalArgumentException("Access token not found");
    }

    JsonElement expiresInElement = JSONUtil.toJsonObject(responseString).get("expires_in");
    Date expiresInDate = null;
    if (expiresInElement != null) {
      Instant now = Instant.now();
      Duration expiresIn = Duration.ofSeconds(expiresInElement.getAsInt());
      Duration buffer = Duration.ofMinutes(1);

      Instant expiresAt = now.plus(expiresIn).minus(buffer);
      expiresInDate = Date.from(expiresAt);
    }

    return new AccessToken(accessTokenElement.getAsString(), expiresInDate);
  }

  /**
   * Get the Access Token using the Service Account details from the config object. The AccessToken returned has a
   * valid ExpirationTime set in the returned AccessToken instance.
   * @param config
   * @return
   * @throws IOException
   */
  private static AccessToken getAccessTokenByServiceAccount(BaseHttpConfig config) throws IOException {
    try {
      GoogleCredentials credential;
      ImmutableSet scopeSet = ImmutableSet.of("https://www.googleapis.com/auth/cloud-platform");
      if (config.getServiceAccountScope() != null) {
        String[] scopes = config.getServiceAccountScope().split("\n");
        for (String scope: scopes) {
          scopeSet = ImmutableSet.builder().addAll(scopeSet).add(scope).build();
        }
      }
      credential = getGoogleCredentials(config).createScoped(scopeSet);
      return credential.refreshAccessToken();
    } catch (Exception e) {
      throw new IllegalArgumentException(
              "Failed to generate Credentials with the given Service Account information", e);
    }
  }

  private static GoogleCredentials getGoogleCredentials(BaseHttpConfig config) throws IOException {
    GoogleCredentials credential;
    if (config.isServiceAccountJson()) {
      InputStream jsonInputStream = new ByteArrayInputStream(config.getServiceAccountJson()
              .getBytes(StandardCharsets.UTF_8));
      credential = GoogleCredentials.fromStream(jsonInputStream);
    } else if (config.isServiceAccountFilePath() && !Strings.isNullOrEmpty(config.getServiceAccountFilePath())
            && !BaseHttpSourceConfig.PROPERTY_AUTO_DETECT_VALUE.equals(config.getServiceAccountFilePath())) {
      credential = GoogleCredentials.fromStream(new FileInputStream(config.getServiceAccountFilePath()));
    } else {
      credential = GoogleCredentials.getApplicationDefault();
    }
    return credential;
  }
}

