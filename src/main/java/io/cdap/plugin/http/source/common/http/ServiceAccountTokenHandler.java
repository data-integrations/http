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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import javax.security.sasl.AuthenticationException;

/**
 * Handle Service Account tokens
 */
public class ServiceAccountTokenHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ServiceAccountTokenHandler.class);

    private AccessToken token;
    GoogleCredentials googleCredentials;

    public ServiceAccountTokenHandler(String scopes, String privateKeyJson) throws IOException {
        ServiceAccountCredentials serviceAccountCredentials;

        try (InputStream stream = new ByteArrayInputStream(privateKeyJson.getBytes())) {
            serviceAccountCredentials = ServiceAccountCredentials.fromStream(stream);
        }

        googleCredentials = serviceAccountCredentials
                .createScoped(scopes.split(" "));
    }

    public boolean tokenExpireSoon() {
        if (token == null) {
            return true;
        }
        Date currentDate = new Date();
        Date expirationTime = token.getExpirationTime();
        Date expirationTimeShifted = new Date(expirationTime.getTime() - (5 * 60 * 1000));
        // Remove 5 minutes from the expiration date for more safety

        return currentDate.after(expirationTimeShifted);
    }

    public AccessToken getAccessToken() throws IOException {
        if (token == null) {
            token = googleCredentials.getAccessToken();

            if (token == null) {
                token = googleCredentials.refreshAccessToken();
            }
        } else {
            if (tokenExpireSoon()) {
                token = googleCredentials.refreshAccessToken();
            }
        }

        if (token != null) {
            return token;
        } else {
            throw new AuthenticationException("Tried to get access token from GoogleCrendentials but got " + token);
        }
    }
}
