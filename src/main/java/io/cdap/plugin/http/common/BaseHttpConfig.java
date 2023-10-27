/*
 * Copyright © 2023 Cask Data, Inc.
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

import com.google.auth.oauth2.AccessToken;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.common.ReferencePluginConfig;
import io.cdap.plugin.http.common.http.AuthType;
import io.cdap.plugin.http.common.http.OAuthUtil;
import java.io.File;
import java.util.Optional;
import javax.annotation.Nullable;


/**
 *  Base configuration for HTTP Source and HTTP Sink
 */
public abstract class BaseHttpConfig extends ReferencePluginConfig {

    public static final String PROPERTY_AUTH_TYPE = "authType";
    public static final String PROPERTY_OAUTH2_ENABLED = "oauth2Enabled";
    public static final String PROPERTY_AUTH_URL = "authUrl";
    public static final String PROPERTY_TOKEN_URL = "tokenUrl";
    public static final String PROPERTY_CLIENT_ID = "clientId";
    public static final String PROPERTY_CLIENT_SECRET = "clientSecret";
    public static final String PROPERTY_SCOPES = "scopes";
    public static final String PROPERTY_REFRESH_TOKEN = "refreshToken";

    public static final String PROPERTY_AUTH_TYPE_LABEL = "Auth type";

    public static final String PROPERTY_USERNAME = "username";

    public static final String PROPERTY_PASSWORD = "password";

    public static final String PROPERTY_NAME_SERVICE_ACCOUNT_TYPE = "serviceAccountType";

    public static final String PROPERTY_NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceAccountFilePath";

    public static final String PROPERTY_NAME_SERVICE_ACCOUNT_JSON = "serviceAccountJSON";

    public static final String PROPERTY_SERVICE_ACCOUNT_FILE_PATH = "filePath";

    public static final String PROPERTY_SERVICE_ACCOUNT_JSON = "JSON";

    public static final String PROPERTY_AUTO_DETECT_VALUE = "auto-detect";

    public static final String PROPERTY_SERVICE_ACCOUNT_SCOPE = "serviceAccountScope";

    @Name(PROPERTY_AUTH_TYPE)
    @Description("Type of authentication used to submit request. \n" +
            "OAuth2, Service account, Basic Authentication types are available.")
    protected String authType;

    @Name(PROPERTY_OAUTH2_ENABLED)
    @Description("If true, plugin will perform OAuth2 authentication.")
    @Nullable
    protected String oauth2Enabled;

    @Nullable
    @Name(PROPERTY_AUTH_URL)
    @Description("Endpoint for the authorization server used to retrieve the authorization code.")
    @Macro
    protected String authUrl;

    @Nullable
    @Name(PROPERTY_TOKEN_URL)
    @Description("Endpoint for the resource server, which exchanges the authorization code for an access token.")
    @Macro
    protected String tokenUrl;

    @Nullable
    @Name(PROPERTY_CLIENT_ID)
    @Description("Client identifier obtained during the Application registration process.")
    @Macro
    protected String clientId;

    @Nullable
    @Name(PROPERTY_CLIENT_SECRET)
    @Description("Client secret obtained during the Application registration process.")
    @Macro
    protected String clientSecret;

    @Nullable
    @Name(PROPERTY_SCOPES)
    @Description("Scope of the access request, which might have multiple space-separated values.")
    @Macro
    protected String scopes;

    @Nullable
    @Name(PROPERTY_REFRESH_TOKEN)
    @Description("Token used to receive accessToken, which is end product of OAuth2.")
    @Macro
    protected String refreshToken;

    @Nullable
    @Name(PROPERTY_USERNAME)
    @Description("Username for basic authentication.")
    @Macro
    protected String username;

    @Nullable
    @Name(PROPERTY_PASSWORD)
    @Description("Password for basic authentication.")
    @Macro
    protected String password;

    @Name(PROPERTY_NAME_SERVICE_ACCOUNT_TYPE)
    @Description("Service account type, file path where the service account is located or the JSON content of the " +
            "service account.")
    @Nullable
    protected String serviceAccountType;

    @Nullable
    @Macro
    @Name(PROPERTY_NAME_SERVICE_ACCOUNT_FILE_PATH)
    @Description("Path on the local file system of the service account key used for authorization. " +
            "Can be set to 'auto-detect' for getting service account from system variable. " +
            "The file/system variable must be present on every node in the cluster. " +
            "Service account json can be generated on Google Cloud " +
            "Service Account page (https://console.cloud.google.com/iam-admin/serviceaccounts).")
    protected String serviceAccountFilePath;

    @Name(PROPERTY_NAME_SERVICE_ACCOUNT_JSON)
    @Description("Content of the service account file.")
    @Nullable
    @Macro
    protected String serviceAccountJson;

    @Nullable
    @Name(PROPERTY_SERVICE_ACCOUNT_SCOPE)
    @Description("The additional Google credential scopes required to access entered url, " +
            "cloud-platform is included by default, " +
            "visit https://developers.google.com/identity/protocols/oauth2/scopes " +
            "for more information.")
    @Macro
    protected String serviceAccountScope;

    public BaseHttpConfig(String referenceName) {
        super(referenceName);
    }

    public AuthType getAuthType() {
        return AuthType.fromValue(authType);
    }

    public String getAuthTypeString() {
        return authType;
    }

    public Boolean getOauth2Enabled() {
        return Boolean.parseBoolean(oauth2Enabled);
    }

    public String getOAuth2Enabled() {
        return oauth2Enabled;
    }

    @Nullable
    public String getAuthUrl() {
        return authUrl;
    }

    @Nullable
    public String getTokenUrl() {
        return tokenUrl;
    }

    @Nullable
    public String getClientId() {
        return clientId;
    }

    @Nullable
    public String getClientSecret() {
        return clientSecret;
    }

    @Nullable
    public String getScopes() {
        return scopes;
    }

    @Nullable
    public String getRefreshToken() {
        return refreshToken;
    }

    @Nullable
    public String getUsername() {
        return username;
    }

    @Nullable
    public String getPassword() {
        return password;
    }

    public void setServiceAccountType(String serviceAccountType) {
        this.serviceAccountType = serviceAccountType;
    }

    @Nullable
    public String getServiceAccountType() {
        return serviceAccountType;
    }

    public void setServiceAccountJson(String serviceAccountJson) {
        this.serviceAccountJson = serviceAccountJson;
    }

    public void setAuthType(String authType) {
        this.authType = authType;
    }

    @Nullable
    public String getServiceAccountJson() {
        return serviceAccountJson;
    }

    public void setServiceAccountFilePath(String serviceAccountFilePath) {
        this.serviceAccountFilePath = serviceAccountFilePath;
    }

    @Nullable
    public String getServiceAccountFilePath() {
        return serviceAccountFilePath;
    }

    @Nullable
    public String getServiceAccountScope() {
        return serviceAccountScope;
    }

    @Nullable
    public Boolean isServiceAccountJson() {
        String serviceAccountType = getServiceAccountType();
        return Strings.isNullOrEmpty(serviceAccountType) ? null :
                serviceAccountType.equals(PROPERTY_SERVICE_ACCOUNT_JSON);
    }

    @Nullable
    public Boolean isServiceAccountFilePath() {
        String serviceAccountType = getServiceAccountType();
        return Strings.isNullOrEmpty(serviceAccountType) ? null :
                serviceAccountType.equals(PROPERTY_SERVICE_ACCOUNT_FILE_PATH);
    }

    public boolean validateServiceAccount(FailureCollector collector) {
        if (containsMacro(PROPERTY_NAME_SERVICE_ACCOUNT_FILE_PATH) ||
                containsMacro(PROPERTY_NAME_SERVICE_ACCOUNT_JSON)) {
            return false;
        }
        final Boolean bServiceAccountFilePath = isServiceAccountFilePath();
        final Boolean bServiceAccountJson = isServiceAccountJson();

        // we don't want the validation to fail because the VM used during the validation
        // may be different from the VM used during runtime and may not have the Google Drive Api scope.
        if (bServiceAccountFilePath && PROPERTY_AUTO_DETECT_VALUE.equalsIgnoreCase(serviceAccountFilePath)) {
            return false;
        }

        if (bServiceAccountFilePath != null && bServiceAccountFilePath) {
            if (!PROPERTY_AUTO_DETECT_VALUE.equals(serviceAccountFilePath) &&
                    !new File(serviceAccountFilePath).exists()) {
                collector.addFailure("Service Account File Path is not available.",
                                "Please provide path to existing Service Account file.")
                        .withConfigProperty(PROPERTY_NAME_SERVICE_ACCOUNT_FILE_PATH);
            }
        }
        if (bServiceAccountJson != null && bServiceAccountJson) {
            if (!Optional.ofNullable(getServiceAccountJson()).isPresent()) {
                collector.addFailure("Service Account JSON can not be empty.",
                                "Please provide Service Account JSON.")
                        .withConfigProperty(PROPERTY_NAME_SERVICE_ACCOUNT_JSON);
            }
        }
        return collector.getValidationFailures().size() == 0;
    }

    public void validate(FailureCollector failureCollector) {
        // Validate OAuth2 properties
        if (!containsMacro(PROPERTY_OAUTH2_ENABLED) && this.getOauth2Enabled()) {
            String reasonOauth2 = "OAuth2 is enabled";
            assertIsSet(getTokenUrl(), PROPERTY_TOKEN_URL, reasonOauth2);
            assertIsSet(getClientId(), PROPERTY_CLIENT_ID, reasonOauth2);
            assertIsSet(getClientSecret(), PROPERTY_CLIENT_SECRET, reasonOauth2);
            assertIsSet(getRefreshToken(), PROPERTY_REFRESH_TOKEN, reasonOauth2);
        }

        // Validate Authentication properties
        AuthType authType = getAuthType();
        switch (authType) {
            case OAUTH2:
                String reasonOauth2 = "OAuth2 is enabled";
                if (!containsMacro(PROPERTY_TOKEN_URL)) {
                    assertIsSet(getTokenUrl(), PROPERTY_TOKEN_URL, reasonOauth2);
                }
                if (!containsMacro(PROPERTY_CLIENT_ID)) {
                    assertIsSet(getClientId(), PROPERTY_CLIENT_ID, reasonOauth2);
                }
                if (!containsMacro((PROPERTY_CLIENT_SECRET))) {
                    assertIsSet(getClientSecret(), PROPERTY_CLIENT_SECRET, reasonOauth2);
                }
                if (!containsMacro(PROPERTY_REFRESH_TOKEN)) {
                    assertIsSet(getRefreshToken(), PROPERTY_REFRESH_TOKEN, reasonOauth2);
                }
                break;
            case SERVICE_ACCOUNT:
                String reasonSA = "Service Account is enabled";
                assertIsSet(getServiceAccountType(), PROPERTY_NAME_SERVICE_ACCOUNT_TYPE, reasonSA);
                boolean propertiesAreValid = validateServiceAccount(failureCollector);
                if (propertiesAreValid) {
                    try {
                        AccessToken accessToken = OAuthUtil.getAccessToken(this);
                    } catch (Exception e) {
                        failureCollector.addFailure("Unable to authenticate given service account info. ",
                                        "Please make sure all infomation entered correctly")
                                .withStacktrace(e.getStackTrace());
                    }
                }
                break;
            case BASIC_AUTH:
                String reasonBasicAuth = "Basic Authentication is enabled";
                if (!containsMacro(PROPERTY_USERNAME)) {
                    assertIsSet(getUsername(), PROPERTY_USERNAME, reasonBasicAuth);
                }
                if (!containsMacro(PROPERTY_PASSWORD)) {
                    assertIsSet(getPassword(), PROPERTY_PASSWORD, reasonBasicAuth);
                }
                break;
        }
    }

    public static void assertIsSet(Object propertyValue, String propertyName, String reason) {
        if (propertyValue == null) {
            throw new InvalidConfigPropertyException(
                    String.format("Property '%s' must be set, since %s", propertyName, reason), propertyName);
        }
    }
}
