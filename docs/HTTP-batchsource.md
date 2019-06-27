# HTTP Batch Source

Description
-----------
This plugin reads data from HTTP/HTTPS pages.
Using paginated API to retrieve data is allowed.
Data in JSON, XML, CSV, TSV, TEXT and BLOB formats is supported.

Properties
----------

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

### General

**URL:** Url to fetch to the first page.
The url must start with a protocol (e.g. http://).

Supports {pagination.index} placeholder, neccessary for pagination
type "Increment an index". For more information read pagination section.

**HTTP Method:** Request HTTP method.

**Headers:** Request HTTP headers for the.

**Request body:** A body of the request.

### Format

**Format:** Format of pages. Used to determine how to convert pages into output records. Possible values are:<br>
JSON - retrieves all records from given location (json path)
and transforms them into records according to the mapping.<br>
XML - retrieves all records from given location (XPath)
and transforms them into records according to the mapping.<br>
TSV/CSV - transforms TSV/CSV pages into records. Page columns are mapped to record fields in the order they are
listed in schema.<br>
Text - transforms a single line of text into a single record with a string field "body" containing the result.<br>
BLOB - transforms a single page into a single record with a byte array field "body" containing the result.

**JSON/XML Result Path:** Path to the results. When XML is used this is an XPath, when json is used this is a JSON path.
JSON path examples: "/bookstore/books", "/feed/entries/items". JSON path should either point to an array in a json
structure or to a field within an array element.

**JSON/XML Fields Mapping:** Mapping of fields in a record to fields in retrieved element. The left column contains the
name of schema field. The right column contains path to it within a relative to an element. It can be either XPath or 
JSON path.

Example:

| Field Name      | Field Path                                |
| --------------- |:-----------------------------------------:|
| name            | /key                                      |
| type            | /fields/issuetype/name                    |
| description     | /fields/description                       |
| projectCategory | /fields/project/projectCategory/name      |
| isSubtask       | /fields/issuetype/subtask                 |
| fixVersions     | /fields/fixVersions                       |

**CSV Skip First Row:** If true, skip first row. This should be set in case first row of csv is a header row.

### Basic Authentication

**Username:** Username for basic authentication.

**Password:** Password for basic authentication.

### HTTP Proxy

**Proxy URL:** Proxy URL. Must contain a protocol, address and port.

**Username:** Proxy username.

**Password:** Proxy password.

### Error Handling

**HTTP Errors Handling:** A mapping which defines error handling strategy used, when pages return certain HTTP status
codes. The left column contains a regular expression for http status code. The right column contains an action which
is done in case of match. If http status code matches multiple regular expressions, the first specified in mapping
is matched.

Example:

| HTTP Code Regexp  | Error Handling          |
| ----------------- |:-----------------------:|
| 2..               | Success                 |
| 401               | Retry and fail          |
| 4..               | Fail                    |
| 5..               | Retry and send to error |
| .*                | Fail                    |

Note: pagination types "Link in response header", "Link in response body", "Token in response body" does not support
"Send to error", "Skip", "Retry and send to error", "Retry and skip" options.

**Non-HTTP Error Handling:** Strategy used to handle errors during transformation of a text entry to record.
Possible values are:<br>
Stop on error - Fails pipeline due to erroneous record.<br>
Send to error - Sends erroneous record's text to error port and continues.<br>
Skip on error - Ignores erroneous records.

**Retry Policy:** Policy used to calculate delay between retries.

**Linear Retry Interval:** Interval between retries. Is only used if retry policy is "linear".

**Max Retry Duration:** Maximum time in seconds retries can take.

**Connect Timeout:** Maximum time in seconds connection initialization is allowed to take.

**Read Timeout:** Maximum time in seconds fetching data from the server is allowed to take.

### Pagination

**Pagination Type:** Strategy used to determine how to get next page.

**Wait time between pages:** Interval between loading pages in milliseconds.
<br><br>

##### Pagination type: None
Only single page is loaded.
<br><br>

##### Pagination type: Link in response header
In response there is a "Link" header, which contains an url marked as "next". Example:<br>
`<http://example.cdap.io/admin/api/pages?page=1&q.language.id=1>; rel="first",
 <http://example.cdap.io/admin/api/pages?page=2&q.language.id=1>; rel="next",
 <http://example.cdap.io/admin/api/pages?page=2&q.language.id=1>; rel="last"`
<br><br>

##### Pagination type: Link in response body
Every page contains a next page url. This pagination type is only supported for JSON and XML formats.
Pagination happens until no next page field is present or until page contains no elements.

**Next Page JSON/XML Field Path:** A JSON path or an XPath to a field which contains next page url.
<br><br>

##### Pagination type: Token in response body
Every page contains a token, which is appended as an url parameter to obtain next page.
This type of pagination is only supported for JSON and XML formats. Pagination happens until no next page
token is present on the page or until page contains no elements.

**Next Page Token Path:** A JSON path or an XPath to a field which contains next page token.

**Next Page Url Parameter:** A parameter which is appended to url in order to specify next page token.
<br><br>

##### Pagination type: Increment an index
Pagination by incrementing a {pagination.index} placeholder value in url. For this pagination type url is required
to contain above placeholder.

**Start Index:** Start value of {pagination.index} placeholder

**Max Index:** Maximum value of {pagination.index} placeholder. If empty, pagination will happen until the page with
no elements.

**Index Increment:** A value which the {pagination.index} placeholder is incremented by. Increment can be negative.
<br><br>

##### Pagination type: Custom
Pagination using user provided code. The code decides how to retrieve a next page url based on previous page contents
and headers and when to finish pagination.

**Custom Pagination Python Code:** A code which implements retrieving
a next page url based on previous page contents and headers.

### OAuth2

**OAuth2 Enabled:** If true, plugin will perform OAuth2 authentication.

**Auth URL:** The endpoint for authorization server, which retrieves the authorization code.

**Token URL:** The endpoint for the resource server, which exchanges the authorization code for an access token.

**Client ID:** The client identifier obtained during the Application registration process.

**Client Secret:** The client secret given obtained during the Application registration process.

**Scopes:** The scope of the access request, which might have multiple space-separated values.

**Refresh Token:** The token used to receive accessToken, which is end product of OAuth2.

### SSL/TLS

**Verify HTTPS Trust Certificates:** If false, untrusted trust certificates (e.g. self signed), will not lead to an
error.

**Keystore File:** A path to a file which contains keystore.

**Keystore Type:** Format of a keystore.

**Keystore Password:** Password for a keystore. If a keystore is not password protected leave it empty.

**Keystore Key Algorithm:** An algorithm used for keystore.

**TrustStore File:** A path to a file which contains truststore.

**TrustStore Type:** Format of a truststore.

**TrustStore Password:** Password for a truststore. If a truststore is not password protected leave it empty.

**TrustStore Key Algorithm:** An algorithm used for truststore.

**Transport Protocols:** Transport protocols which are allowed for connection.

**Cipher Suites:** Cipher suites which are allowed for connection.

**Schema:** Output schema. Is required to be set.