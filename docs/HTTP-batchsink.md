# HTTP Sink


Description
-----------
Sink plugin to send the messages from the pipeline to an external http endpoint.

Properties
----------

**url:** The URL to post data to.

**method:** The HTTP request method. Defaults to POST.

**bathSize:** Batch size. Defaults to 1.

**messageFormat:** Format to send messsage in. Options are JSON,Form,Custom. Defaults to JSON.

**body:** Custom message. This would be mandatory if message format is selected as Custom.
          In case user wants to have Custom message and want to use some of the input record fields as variables to build the message,
          then user needs to put the variable with # as prefix in the message so that the same would be replaced by the value from the input record.

**delimiterForMessages:** Delimiter for messages in case of batching > 1. Defaults to "\n".

**requestHeaders:** An optional string of header values to send in each request where the keys and values are
delimited by a colon (":") and each pair is delimited by a newline ("\n").

**charset:** Charset. Defaults to UTF-8.

**followRedirects:** Whether to automatically follow redirects. Defaults to true.

**disableSSLValidation:**  If user enables SSL validation, they will be expected to add the certificate to the trustStore on each machine. Defaults to true.

**numRetries:** The number of times the request should be retried if the request fails. Defaults to 3.

**connectTimeout:** The time in milliseconds to wait for a connection. Set to 0 for infinite. Defaults to 60000 (1 minute).

**readTimeout:** The time in milliseconds to wait for a read. Set to 0 for infinite. Defaults to 60000 (1 minute).

**failOnNon200Response** Whether to fail the pipeline on non-200 response from the http end point. Defaults to true.

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
