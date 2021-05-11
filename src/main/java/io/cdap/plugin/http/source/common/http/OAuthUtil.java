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

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.UrlEncodedContent;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebToken;
import com.google.api.client.util.GenericData;
import com.google.api.client.util.SecurityUtils;

import com.google.auth.http.HttpTransportFactory;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.cdap.plugin.http.source.common.pagination.page.JSONUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;


import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;

import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class which contains utilities to make OAuth2 specific calls.
 */
public class OAuthUtil {

  public static PrivateKey readPKCS8Pem(String key) throws Exception {
        key = key.replace("-----BEGIN PRIVATE KEY-----", "");
        key = key.replace("-----END PRIVATE KEY-----", "");
        key = key.replaceAll("\\s+", "");

        // Base64 decode the result
        byte [] pkcs8EncodedBytes = decode(key);

        // extract the private key
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(pkcs8EncodedBytes);
        KeyFactory kf = KeyFactory.getInstance("RSA");
        return kf.generatePrivate(keySpec);
    }

  public static String encodeBase64URLSafeString(byte[] binaryData) {
    if (binaryData == null) {
      return null;
    }
    return Base64.getUrlEncoder().withoutPadding().encodeToString(binaryData);
  }

  public static String signUsingRsaSha256(
      PrivateKey privateKey,
      JsonFactory jsonFactory,
      JsonWebSignature.Header header,
      JsonWebToken.Payload payload)
      throws GeneralSecurityException, IOException {
    String head = encodeBase64URLSafeString(jsonFactory.toByteArray(header));
    String content = head + "."
            + encodeBase64URLSafeString(jsonFactory.toByteArray(payload));
    byte[] contentBytes = content.getBytes();
    Signature sig = Signature.getInstance("SHA256withRSA");
    sig.initSign(privateKey);
    sig.update(contentBytes);
    byte[] signature = sig.sign();
     return content + "." + encodeBase64URLSafeString(signature);
  }



// copied from https://stackoverflow.com/a/4265472
    private static char[] theALPHABET = 
         "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".toCharArray();

    private static int[]  toInt   = new int[128];

    static {
        for (int i = 0; i < theALPHABET.length; i++) {
            toInt[theALPHABET[i]] = i;
        }
    }

    /**
     * Translates the specified byte array into Base64 string.
     *
     * @param buf the byte array (not null)
     * @return the translated Base64 string (not null)
     */
    public static String encode(byte[] buf) {
        int size = buf.length;
        char[] ar = new char[((size + 2) / 3) * 4];
        int a = 0;
        int i = 0;
        while (i < size) {
            byte b0 = buf[i++];
            byte b1 = (i < size) ? buf[i++] : 0;
            byte b2 = (i < size) ? buf[i++] : 0;

            int mask = 0x3F;
            ar[a++] = theALPHABET[(b0 >> 2) & mask];
            ar[a++] = theALPHABET[((b0 << 4) | ((b1 & 0xFF) >> 4)) & mask];
            ar[a++] = theALPHABET[((b1 << 2) | ((b2 & 0xFF) >> 6)) & mask];
            ar[a++] = theALPHABET[b2 & mask];
        }
        switch (size % 3) {
            case 1: ar[--a]  = '=';
                    break;
            case 2: ar[--a]  = '=';
                    ar[--a]  = '=';
                    break;
        }
        return new String(ar);
    }

    /**
     * Translates the specified Base64 string into a byte array.
     *
     * @param s the Base64 string (not null)
     * @return the byte array (not null)
     */
    public static byte[] decode(String s) {
        int delta = s.endsWith("==") ? 2 : s.endsWith("=") ? 1 : 0;
        byte[] buffer = new byte[s.length() * 3 / 4 - delta];
        int mask = 0xFF;
        int index = 0;
        for (int i = 0; i < s.length(); i += 4) {
            int c0 = toInt[s.charAt(i)];
            int c1 = toInt[s.charAt(i + 1)];
            buffer[index++] = (byte) (((c0 << 2) | (c1 >> 4)) & mask);
            if (index >= buffer.length) {
                return buffer;
            }
            int c2 = toInt[s.charAt(i + 2)];
            buffer[index++] = (byte) (((c1 << 4) | (c2 >> 2)) & mask);
            if (index >= buffer.length) {
                return buffer;
            }
            int c3 = toInt[s.charAt(i + 3)];
            buffer[index++] = (byte) (((c2 << 6) | c3) & mask);
        }
        return buffer;
    }


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

  public static String getAccessTokenByServiceAccount(CloseableHttpClient httpclient, String serviceAccountJson,
                                                      String serviceAccountScope)
    throws IOException {
    HttpTransportFactory transportFactory;
    JsonObject sa = JSONUtil.toJsonObject(serviceAccountJson);
    String tokenServerUri = sa.get("token_uri").getAsString();
    String scope = serviceAccountScope;

    if (serviceAccountScope == null) {
      scope = "https://www.googleapis.com/auth/cloud-platform";
    }

    PrivateKey key;
    try {
      key = readPKCS8Pem(sa.get("private_key").getAsString());
    } catch (Exception e) {
      throw new IOException(
          "Error decoding service account private key.", e);
    }
    long currentTime = System.currentTimeMillis();
    JsonWebSignature.Header header = new JsonWebSignature.Header();
    header.setAlgorithm("RS256");
    header.setType("JWT");
    header.setKeyId(sa.get("private_key_id").getAsString());


    JsonWebToken.Payload payload = new JsonWebToken.Payload();
    payload.setIssuer(sa.get("client_email").getAsString());
    payload.setIssuedAtTimeSeconds(currentTime / 1000);
    payload.setExpirationTimeSeconds(currentTime / 1000 + 3600);  // one hour
    payload.setSubject(sa.get("client_email").getAsString());
    payload.put("scope", scope);
    payload.setAudience(tokenServerUri);

    String assertion;
    try {
      assertion = signUsingRsaSha256(key, GsonFactory.getDefaultInstance(), header, payload);
    } catch (GeneralSecurityException e) {
      throw new IOException(
          "Error signing service account access token request with private key.", e);
    }

    GenericData tokenRequest = new GenericData();
    tokenRequest.set("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer");
    tokenRequest.set("assertion", assertion);

    UrlEncodedContent content = new UrlEncodedContent(tokenRequest);
    HttpRequestFactory requestFactory = new NetHttpTransport().createRequestFactory();
    HttpRequest request = requestFactory.buildPostRequest(new GenericUrl(tokenServerUri), content);
    HttpResponse response = request.execute();

    InputStream in = response.getContent();
    StringWriter out = new StringWriter();
    byte[] buf = new byte[4096];
    int r;
    while (true) {
      r = in.read(buf);
      if (r == -1) {
        break;
      }
      out.write(new String(buf).substring(0, r));
    }
    JsonObject responseData = JSONUtil.toJsonObject(out.toString());
    return responseData.get("access_token").toString();
  }
}
