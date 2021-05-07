# HTTP Batch Source

Description
-----------
This plugin reads data from HTTP/HTTPS pages.
Paginated APIs are supported.
Data in JSON, XML, CSV, TSV, TEXT and BLOB formats is supported.

Properties
----------

### General

**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**URL:** Url to fetch to the first page.
The url must start with a protocol (e.g. http://).

**HTTP Method:** HTTP request method.

**Headers:** Headers to send with each HTTP request.

**Request body:** Body to send with each HTTP request.

### Format

**Format:** Format of the HTTP response. This determines how the response is converted into output records. Possible values are:<br>
JSON - retrieves all records from the given json path
and transforms them into records according to the mapping.<br>
XML - retrieves all records from the given XPath
and transforms them into records according to the mapping.<br>
TSV - tab separated values. Columns are mapped to record fields in the order they are
listed in schema.<br>
CSV - comma separated values. Columns are mapped to record fields in the order they are
listed in schema.<br>
Text - transforms a single line of text into a single record with a string field `body` containing the result.<br>
BLOB - transforms the entire response into a single record with a byte array field `body` containing the result.

**JSON/XML Result Path:** Path to the results. When the format is XML, this is an XPath. When the format is JSON, this is a JSON path.

JSON path example:
```
{
     "errors": [],
     "response": {
       "books": [
         {
           "id": "1159142",
           "title": "Agile Web Development with Rails",
           "author": "Sam Ruby, Dave Thomas, David Heinemeier Hansson",
           "printInfo": {
             "page": 488,
             "coverType": "hard",
             "publisher": "Pragmatic Bookshelf"
           }
         },
         {
           "id": "2375753",
           "title": "Flask Web Development",
           "author": "Miguel Grinberg",
           "printInfo": {
             "page": 543,
             "coverType": "hard",
             "publisher": "O'Reilly Media, Inc"
           }
         },
         {
           "id": "547307",
           "title": "Alex Homer, ASP.NET 2.0 Visual Web Developer 2005",
           "author": "David Sussman",
           "printInfo": {
             "page": 543,
             "coverType": "hard",
             "publisher": "unknown"
           }
         }
       ]
     }
}
 ```
Json path to fetch books is `/response/books`. However if we need to fetch only `printInfo` we can specify
`/response/books/printInfo` as well.

XPath example:
```
<?xml version="1.0" encoding="UTF-8"?>
<bookstores>
  <bookstore id="1">
     <book category="cooking">
        <title lang="en">Everyday Italian</title>
        <author>Giada De Laurentiis</author>
        <year>2005</year>
        <price>
         <value>15.0</value>
         <policy>Discount up to 50%</policy>
        </price>
     </book>
     <book category="web">
        <title lang="en">XQuery Kick Start</title>
        <author>James McGovern</author>
        <author>Per Bothner</author>
        <year>2003</year>
        <price>
         <value>49.99</value>
         <policy>No discount</policy>
        </price>
     </book>
     ...
  </bookstore>
  <bookstore id="2">
     ...
  </bookstore>
</bookstores>
```

XPath to fetch all books is `/bookstores/bookstore/book`. However a more precise selections can be done. E.g.
`/bookstores/bookstore/book[@category='web']`.

**JSON/XML Fields Mapping:** Mapping of fields in a record to fields in retrieved element. The left column contains the
name of schema field. The right column contains path to it within a relative to an element. It can be either XPath or 
JSON path.

Example response:
```
{
   "startAt":1,
   "maxResults":5,
   "total":15599,
   "issues":[
      {
         "id":"20276",
         "key":"NETTY-14",
         "fields":{
            "issuetype":{
               "name":"Bug",
               "subtask":false
            },
            "fixVersions":[
               "4.1.37"
            ],
            "description":"Test description for NETTY-14",
            "project":{
               "id":"10301",
               "key":"NETTY",
               "name":"Netty-HTTP",
               "projectCategory":{
                  "id":"10002",
                  "name":"Infrastructure"
               }
            }
         }
      },
      {
         "id":"19124",
         "key":"NETTY-13",
         "fields":{
            "issuetype":{
               "self":"https://issues.cask.co/rest/api/2/issuetype/4",
               "name":"Improvement",
               "subtask":false
            },
            "fixVersions":[

            ],
            "description":"Test description for NETTY-13",
            "project":{
               "id":"10301",
               "key":"NETTY",
               "name":"Netty-HTTP",
               "projectCategory":{
                  "id":"10002",
                  "name":"Infrastructure"
               }
            }
         }
      }
   ]
}
```

Assume the result path is `/issues`.

The mapping is:

| Field Name      | Field Path                                |
| --------------- |:-----------------------------------------:|
| type            | /fields/issuetype/name                    |
| description     | /fields/description                       |
| projectCategory | /fields/project/projectCategory/name      |
| isSubtask       | /fields/issuetype/subtask                 |
| fixVersions     | /fields/fixVersions                       |

The result records are:

| key	     | type	       | isSubtask | description	                 | projectCategory	| fixVersions |
| -------- | ----------- | --------- | ----------------------------- | ---------------- | ----------- |
| NETTY-14 | Bug         | false     | Test description for NETTY-14 | Infrastructure	  | ["4.1.37"]  |
| NETTY-13 | Improvement | false     | Test description for NETTY-13 | Infrastructure   |	[]          |

Note, that field `key` was mapped without being included into the mapping. Mapping entries like `key: /key`
can be omitted as long as the field is present in schema.
<br>

**CSV Skip First Row:** Whether to skip the first row of the HTTP response. This is usually set if the first row is a header row.

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

**Retry Policy:** Policy used to calculate delay between retries.

**Linear Retry Interval:** Interval between retries. Is only used if retry policy is "linear".

**Max Retry Duration:** Maximum time in seconds retries can take.

**Connect Timeout:** Maximum time in seconds connection initialization is allowed to take.

**Read Timeout:** Maximum time in seconds fetching data from the server is allowed to take.

### Pagination

**Pagination Type:** Strategy used to determine how to get next page.

**Wait Time Between Pages:** Time in milliseconds to wait between HTTP requests for the next page.
<br><br>

##### Pagination type: None
Only single page is loaded.
<br>
##### Pagination type: Link in response header
In response there is a "Link" header, which contains an url marked as "next". Example:<br>
```
<http://example.cdap.io/admin/api/pages?page=1&q.language.id=1>; rel="first",
<http://example.cdap.io/admin/api/pages?page=2&q.language.id=1>; rel="next",
<http://example.cdap.io/admin/api/pages?page=2&q.language.id=1>; rel="last"`
```
<br>

##### Pagination type: Link in response body
Every page contains a next page url. This pagination type is only supported for JSON and XML formats.
Pagination happens until no next page field is present or until page contains no elements.

**Next Page JSON/XML Field Path:** A JSON path or an XPath to a field which contains next page url.
It can be either relative or absolute url.

Example page response:
```
{
  "results": [
    ...
  ]
  "_links": {
    "self": "https://confluence.atlassian.com/rest/api/space/ADMINJIRASERVER0710/content/page",
    "next": "/rest/api/space/ADMINJIRASERVER0710/content/page?limit=100&start=100",
    "base": "https://confluence.atlassian.com",
    "context": ""
  }
}
```
Next page field path is `_links/next`.
<br>
##### Pagination type: Token in response body
Every page contains a token, which is appended as an url parameter to obtain next page.
This type of pagination is only supported for JSON and XML formats. Pagination happens until no next page
token is present on the page or until page contains no elements.

**Next Page Token Path:** A JSON path or an XPath to a field which contains next page token.

**Next Page Url Parameter:** A parameter which is appended to url in order to specify next page token.

Example plugin config:
```
{
  "url": "https://www.googleapis.com/youtube/v3/search?part=snippet&maxResults=20&q=cask+cdap",
  "resultPath": "/items"
  "paginationType": "Token in response body",
  "nextPageTokenPath": "/nextPageToken",
  "nextPageUrlParameter": "pageToken"
}
```

First page response:
```
{
 "nextPageToken": "CAEQAA",
 "pageInfo": {
  "totalResults": 208,
  "resultsPerPage": 2
 },
 "items": [
  ...
 ]
}
```
Next page fetched by plugin will be url with `&pageToken=CAEQAA` appended.
<br>
##### Pagination type: Increment an index
Pagination by incrementing a {pagination.index} placeholder value in url. For this pagination type url is required
to contain above placeholder.

**Start Index:** Start value of {pagination.index} placeholder

**Max Index:** Maximum value of {pagination.index} placeholder. If empty, pagination will happen until the page with
no elements.

**Index Increment:** A value which the {pagination.index} placeholder is incremented by. Increment can be negative.
<br>
##### Pagination type: Custom
Pagination using user provided code. The code decides how to retrieve a next page url based on previous page contents
and headers and when to finish pagination.

**Custom Pagination Python Code:** A code which implements retrieving
a next page url based on previous page contents and headers.

Example code:
```
import json

def get_next_page_url(url, page, headers):
    """
    Based on previous page data generates next page url, when "Custom pagination" is enabled.

    Args:
        url (string): previous page url
        page (string): a body of previous page
        headers (dict): a dictionary of headers from previous page

    """
    page_json = json.loads(page)
    next_page_num = page_json['nextpage']

    # stop the iteration
    if next_page_num == None or next_page_num > 5:
      return None

    return "https://searchcode.com/api/codesearch_I/?q=curl&p={}".format(next_page_num)
```
The above code iterates over first five pages of searchcode.com results. When 'None' is returned the iteration
is stopped.

### OAuth2

**OAuth2 Enabled:** If true, plugin will perform OAuth2 authentication.

**Auth URL:** Endpoint for the authorization server used to retrieve the authorization code.

**Token URL:** Endpoint for the resource server, which exchanges the authorization code for an access token.

**Client ID:** Client identifier obtained during the Application registration process.

**Client Secret:** Client secret obtained during the Application registration process.

**Scopes:** Scope of the access request, which might have multiple space-separated values.

**Refresh Token:** Token used to receive accessToken, which is end product of OAuth2.

### Hawk Authentication

**HAWK Authentication Enabled:** If true, plugin will perform HAWK authentication.

**HAWK Auth ID:** HAWK Authentication ID

**Hawk Auth Key:** HAWK Authentication Key

**Algorithm:** Hash Algorithm used

**ext:** Any application-specific information to be sent with the request. Ex: some-app-extra-data

**app:** This provides binding between the credentials and the application in a way that prevents an attacker from ticking an application to use credentials issued to someone else.

**dlg:** The application id of the application the credentials were directly issued to.

**Include Payload Hash:** HAWK authentication provides optional support for payload validation. If this option is selected, the payload hash will be calculated and included in MAC calculation and in Authorization header

### SSL/TLS

**Verify HTTPS Trust Certificates:** If false, untrusted trust certificates (e.g. self signed), will not lead to an
error. Do not disable this in production environment on a network you do not entirely trust. Especially public internet.

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
Colons, commas or spaces are also acceptable separators.

**Schema:** Output schema. Is required to be set.
