# HTTP Action

Description
-----------
This plugin executing HTTP/HTTPS requests

Properties
----------

### General

**URL:** Url to fetch to the first page.
The url must start with a protocol (e.g. http://).

**HTTP Method:** HTTP request method.

**Headers:** Headers to send with each HTTP request.

**Request body:** Body to send with each HTTP request.


### Basic Authentication

**Username:** Username for basic authentication.

**Password:** Password for basic authentication.

### HTTP Proxy

**Proxy URL:** Proxy URL. Must contain a protocol, address and port.

**Username:** Proxy username.

**Password:** Proxy password.

### Error Handling

**HTTP Errors Handling:** Defines the error handling strategy to use for certain HTTP response codes.
The left column contains a regular expression for HTTP status code. The right column contains an action which
is done in case of match. If HTTP status code matches multiple regular expressions, the first specified in mapping
is matched.

Example:

| HTTP Code Regexp  | Error Handling          |
| ----------------- |:-----------------------:|
| 2..               | Success                 |
| 401               | Retry and fail          |
| 4..               | Fail                    |
| 5..               | Retry and send to error |
| .*                | Fail                    |

Note: pagination types "Link in response header", "Link in response body", "Token in response body" do not support
"Send to error", "Skip", "Retry and send to error", "Retry and skip" options.

**Non-HTTP Error Handling:** Error handling strategy to use when the HTTP response cannot be transformed to an output record.
Possible values are:<br>
Stop on error - Fails pipeline due to erroneous record.<br>
Send to error - Sends erroneous record's text to error port and continues.<br>
Skip on error - Ignores erroneous records.

**Connect Timeout:** Maximum time in seconds connection initialization is allowed to take.

### OAuth2

**OAuth2 Enabled:** If true, plugin will perform OAuth2 authentication.

**Auth URL:** Endpoint for the authorization server used to retrieve the authorization code.

**Token URL:** Endpoint for the resource server, which exchanges the authorization code for an access token.

**Client ID:** Client identifier obtained during the Application registration process.

**Client Secret:** Client secret obtained during the Application registration process.

**Scopes:** Scope of the access request, which might have multiple space-separated values.

**Refresh Token:** Token used to receive accessToken, which is end product of OAuth2.

### SSL/TLS

**Verify HTTPS Trust Certificates:** If false, untrusted trust certificates (e.g. self signed), will not lead to an
error. Do not disable this in production environment on a network you do not entirely trust. Especially public internet.

**Keystore File:** A path to a file which contains keystore.

**Keystore Type:** Format of a keystore.

**Keystore Password:** Password for a keystore. If a keystore is not password protected leave it empty.

**Keystore Key Algorithm:** An algorithm used for keystore.

**Keystore Cert Alias**

 Alias of the key in the keystore to be used for communication. This options is supported only by X.509 keys or keystores.
 
Below is an example how the store need to be prepared:
 ```
 cat  client.crt client.key > client-bundle.pem

 openssl pkcs12 -export -in client-bundle.pem -out full-chain.keycert.p12 -name ${CERT_ALIAS}

 keytool -importkeystore -srckeystore full-chain.keycert.p12 -srcstoretype pkcs12 -srcalias ${CERT_ALIAS} \
                         -destkeystore identity.jks -deststoretype jks -destalias ${CERT_ALIAS}
 ```

**TrustStore File:** A path to a file which contains truststore.

**TrustStore Type:** Format of a truststore.

**TrustStore Password:** Password for a truststore. If a truststore is not password protected leave it empty.

**TrustStore Key Algorithm:** An algorithm used for truststore.

**Transport Protocols:** Transport protocols which are allowed for connection.

**Cipher Suites:** Cipher suites which are allowed for connection.
Colons, commas or spaces are also acceptable separators.

