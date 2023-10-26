# HTTP Sink


Description
-----------
Sink plugin to send the messages from the pipeline to an external http endpoint.

Properties
----------

**url:** The URL to post data to. (Macro enabled)

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

**numRetries:** The number of times the request should be retried if the request fails. Defaults to 3. (Macro enabled)

**connectTimeout:** The time in milliseconds to wait for a connection. Set to 0 for infinite. Defaults to 60000 (1 minute). (Macro enabled)

**readTimeout:** The time in milliseconds to wait for a read. Set to 0 for infinite. Defaults to 60000 (1 minute). (Macro enabled)

**failOnNon200Response** Whether to fail the pipeline on non-200 response from the http end point. Defaults to true. (Macro enabled)

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
            "numRetries": 0,
            "connectTimeout": 60000,
            "readTimeout": 60000
        }
    }
