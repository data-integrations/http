package io.cdap.plugin.http.source.common.http;

import com.wealdtech.hawk.HawkClient;
import com.wealdtech.hawk.HawkCredentials;
import org.apache.http.entity.StringEntity;

import java.net.URI;

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
            String dlg){
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
