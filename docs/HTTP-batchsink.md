# HTTP Sink


Description
-----------
Sink plugin to send the messages from the pipeline to an external http endpoint.

Properties
----------

**url:** The URL to post data to. Additionally, a placeholder like #columnName can be added to the URL that can be 
substituted with column value at the runtime. E.g. https://customer-url/user/#user_id. Here user_id column should exist 
in input schema. (Macro enabled)

**method:** The HTTP request method. Defaults to POST. (Macro enabled)

**batchSize:** Batch size. Defaults to 1. (Macro enabled)

**Write JSON As Array:** Whether to write the JSON as an array. Defaults to false. (Macro enabled)

When set to true, the payload will be written as an array of JSON objects.
Example - If batch size is 2, then the payload will be `[{"key":"val"}, {"key":"val"}]`

When false, the payload will be JSON objects separated by a delimiter.
Example - If batch size is 2, delimiter is "\n" , then the payload will be `{"key":"val"}\n{"key":"val"}`

**Json Batch Key** Optional key to be used for wrapping json array as object
Leave empty for no wrapping of the array. Ignored if Write JSON As Array is set to false. (Macro Enabled)

Example - If batch size is 2 and json batch key is "data", then the payload will be
`{"data": [{"key":"val"}, {"key":"val"}]}` instead of `[{"key":"val"}, {"key":"val"}]`

**messageFormat:** Format to send messsage in. Options are JSON, Form, Custom. Defaults to JSON. (Macro enabled)

**body:** Optional custom message. This is required if the message format is set to 'Custom'.
          User can leverage incoming message fields in the post payload.
          For example-
          User has defined payload as \{ "messageType" : "update", "name" : "#firstName" \}
          where #firstName will be substituted for the value that is in firstName in the incoming message. (Macro enabled)

**delimiterForMessages:** Delimiter for messages in case of batching > 1. Defaults to "\n". (Macro enabled)

**requestHeaders:** An optional string of header values to send in each request where the keys and values are
delimited by a colon (":") and each pair is delimited by a newline ("\n"). (Macro enabled)

**charset:** Charset. Defaults to UTF-8. (Macro enabled)

**followRedirects:** Whether to automatically follow redirects. Defaults to true. (Macro enabled)

**disableSSLValidation:**  If user enables SSL validation, they will be expected to add the certificate to the trustStore on each machine. Defaults to true. (Macro enabled)

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
Possible values are:  
Stop on error - Fails pipeline due to erroneous record.  
Send to error - Sends erroneous record's text to error port and continues.  
Skip on error - Ignores erroneous records.

**Retry Policy:** Policy used to calculate delay between retries. Default Retry Policy is Exponential.

**Linear Retry Interval:** Interval in seconds between retries. Is only used if retry policy is "linear".

**Max Retry Duration:** Maximum time in seconds retries can take. Default value is 600 seconds (10 minute).

**connectTimeout:** The time in milliseconds to wait for a connection. Set to 0 for infinite. Defaults to 60000 (1 minute). (Macro enabled)

**readTimeout:** The time in milliseconds to wait for a read. Set to 0 for infinite. Defaults to 60000 (1 minute). (Macro enabled)

**Wait Time Between Request:** Time in milliseconds to wait between HTTP requests. Defaults to 0. (Macro enabled)

### Authentication

* **OAuth2**
    * **Grant Type:** Which OAuth2 grant type flow is used. It can be Refresh Token or Client Credentials Flow.
    * **Client Authentication:** Send OAuth2 Credentials in the Request Body or as Query Parameter or as Basic Auth
      Header.
    * **Auth URL:** Endpoint for the authorization server used to retrieve the authorization code.
    * **Token URL:** Endpoint for the resource server, which exchanges the authorization code for an access token.
    * **Client ID:** Client identifier obtained during the Application registration process.
    * **Client Secret:** Client secret obtained during the Application registration process.
    * **Scopes:** Scope of the access request, which might have multiple space-separated values.
    * **Refresh Token:** Token used to receive accessToken, which is end product of OAuth2.
* **Service Account** - service account key used for authorization
    * **File Path**: Path on the local file system of the service account key used for
      authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
      When running on other clusters, the file must be present on every node in the cluster.
    * **JSON**: Contents of the service account JSON file.
    * **Scope**: The additional Google credential scopes required to access entered url, cloud-platform is included by
      default, visit https://developers.google.com/identity/protocols/oauth2/scopes for more information.
        * Scope example:

```
https://www.googleapis.com/auth/bigquery
https://www.googleapis.com/auth/cloud-platform
```

* **Basic Authentication**
    * **Username:** Username for basic authentication.
    * **Password:** Password for basic authentication.

### HTTP Proxy

**Proxy URL:** Proxy URL. Must contain a protocol, address and port.

**Username:** Proxy username.

**Password:** Proxy password.

Example
-------
This example performs HTTP POST request to http://example.com/data.

    {
        "name": "HTTP",
        "type": "batchsink",
        "properties": {
            "url": "http://example.com/data",
            "method": "POST",
            "messageFormat": "JSON",
            "batchSize": "1",
            "charset": "UTF-8",
            "followRedirects": "true",
            "disableSSLValidation": "true",
            "connectTimeout": 60000,
            "readTimeout": 60000
        }
    }
