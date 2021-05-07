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

import com.wealdtech.hawk.HawkClient;
import com.wealdtech.hawk.HawkCredentials;
import org.apache.http.entity.StringEntity;

import java.net.URI;

/**
 * A class which contains utilities to make HAWK specific calls.
 */
public class HawkUtil {

    public static HawkClient createHawkClient(String authID, String authKey, HawkCredentials.Algorithm algorithm) {
        HawkCredentials hawkCredentials = new HawkCredentials.Builder()
                .keyId(authID)
                .key(authKey)
                .algorithm(algorithm)
                .build();

        return new HawkClient.Builder().credentials(hawkCredentials).build();
    }

    public static String getAuthorizationHeader(
            HawkClient hawkClient,
            StringEntity requestBody,
            URI uri,
            String method,
            boolean payloadHashEnabled,
            String ext,
            String app,
            String dlg) {
        String hash = null;
        if (payloadHashEnabled) {
            hash = Integer.toString(requestBody.hashCode());
        }

        return hawkClient.generateAuthorizationHeader(
                uri,
                method,
                hash,
                ext,
                app,
                dlg);
    }
}
