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
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.pagination.page.JSONUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
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
          return OAuthUtil.getAccessTokenByRefreshToken(client, config);
        }
    }
    return null;
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
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to build token URI for OAuth2", e);
    }

    HttpPost httppost = new HttpPost(uri);
    CloseableHttpResponse response = httpclient.execute(httppost);
    String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");

    JsonElement accessTokenElement = JSONUtil.toJsonObject(responseString).get("access_token");
    if (accessTokenElement == null) {
      throw new IllegalArgumentException("Access token not found");
    }

    JsonElement expiresInElement = JSONUtil.toJsonObject(responseString).get("expires_in");
    Date expiresInDate = null;
    if (expiresInElement != null) {
      long expiresAtMilliseconds = System.currentTimeMillis()
              + (long) (expiresInElement.getAsInt() * 1000) - 60000L;
      expiresInDate = new Date(expiresAtMilliseconds);
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

