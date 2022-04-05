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
package io.cdap.plugin.http.source.common.http;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import io.cdap.plugin.http.source.common.BaseHttpSourceConfig;
import io.cdap.plugin.http.source.common.pagination.page.JSONUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.interfaces.RSAPrivateKey;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * A class which contains utilities to make OAuth2 specific calls.
 */
public class OAuthUtil {
  public static String getAccessTokenByRefreshToken(CloseableHttpClient httpclient, String tokenUrl, String clientId,
                                                    String clientSecret, String refreshToken)
    throws IOException {

    URI uri;
    try {
      uri = new URIBuilder(tokenUrl)
        .setParameter("client_id", clientId)
        .setParameter("client_secret", clientSecret)
        .setParameter("refresh_token", refreshToken)
        .setParameter("grant_type", "refresh_token")
        .build();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Failed to build token URI for OAuth2", e);
    }

    HttpPost httppost = new HttpPost(uri);
    CloseableHttpResponse response = httpclient.execute(httppost);
    String responseString = EntityUtils.toString(response.getEntity(), "UTF-8");

    JsonElement jsonElement = JSONUtil.toJsonObject(responseString).get("access_token");
    return jsonElement.getAsString();
  }

  public static String getAccessTokenByServiceAccount(BaseHttpSourceConfig config) throws IOException {
    GoogleCredentials credential;
    String accessToken = "";
    try {
      if (config.isServiceAccountJson()) {
        InputStream jsonInputStream = new ByteArrayInputStream(config.getServiceAccountJson()
                                                                 .getBytes(StandardCharsets.UTF_8));
        credential = GoogleCredentials.fromStream(jsonInputStream)
          .createScoped(ImmutableSet.of("https://www.googleapis.com/auth/cloud-platform"));
        accessToken = credential.refreshAccessToken().getTokenValue();
      } else if (config.isServiceAccountFilePath() && !Strings.isNullOrEmpty(config.getServiceAccountFilePath())) {
        credential = GoogleCredentials.fromStream(new FileInputStream(config.getServiceAccountFilePath()))
          .createScoped(ImmutableSet.of("https://www.googleapis.com/auth/cloud-platform"));
        accessToken = credential.refreshAccessToken().getTokenValue();
      } else {
        credential = GoogleCredentials.getApplicationDefault()
          .createScoped(ImmutableSet.of("https://www.googleapis.com/auth/cloud-platform"));
        accessToken = credential.refreshAccessToken().getTokenValue();
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to generate Access Token with given Service Account information", e);
    }
    return accessToken;
  }

  /**
   * Generates a signed JSON Web Token using a Google API Service Account
   * utilizes com.auth0.jwt.
   * https://cloud.google.com/endpoints/docs/openapi/service-account-authentication
   */
  public static String generateJwt(final int expiryLength, GoogleCredentials cred)
    throws IOException {

    Date now = new Date();
    Date expTime = new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(expiryLength));
    String saEmail = ((ServiceAccountCredentials) cred).getClientEmail();

    // Build the JWT payload
    JWTCreator.Builder token = JWT.create()
      .withIssuedAt(now)
      // Expires after 'expiryLength' seconds
      .withExpiresAt(expTime)
      // Must match 'issuer' in the security configuration in your
      // swagger spec (e.g. service account email)
      .withIssuer(saEmail)
      // Must be either your Endpoints service name, or match the value
      // specified as the 'x-google-audience' in the OpenAPI document
      // .withAudience(audience)
      // Subject and email should match the service account's email
      .withSubject(saEmail)
      .withClaim("email", saEmail);

    // Sign the JWT with a service account
    RSAPrivateKey key = (RSAPrivateKey) ((ServiceAccountCredentials) cred).getPrivateKey();
    Algorithm algorithm = Algorithm.RSA256(null, key);
    return token.sign(algorithm);
  }
}

