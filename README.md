# Introduction
Java client library for the <a href="http://docs.h2o.ai/h2o/latest-stable/h2o-docs/index.html">H2O machine learning platform</a>.  In
addition to providing an interface for training and downloading models, this library facilitates the use of Rapids 
expressions for data munging and feature generation by means of the methods provided in the H2OFrame class. The classes in this library
make use of the H2O REST API to provide full access to H2O's capabilities as a distributed, parallel, in-memory data processing
engine from any Java application.

Developed for AT&T by Chris Ranella, July 2018

# Requirements
H2O version 3.2.x

See latest release here: 
<br>http://h2o-release.s3.amazonaws.com/h2o/rel-slater/3/index.html 

# Installation
Build using maven and import jar into your Java project
<br>(https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html)

# Configuration
Use H2OConnection.newInstance(HttpUrl url) to connect to an existing H2O cluster

#### example:
```
HttpUrl url = new HttpUrl.Builder().scheme("http").host("localhost").port(54321).build();
try(H2OConnection conn = H2OConnection.newInstance(url)) {
  YOUR CODE HERE
} catch(H2OException e) {
  ...
}
```

# Help
Javadoc:
https://cranella.github.io/h2oclient-java/

Gitter H2O Developer Chat:
https://gitter.im/h2oai/h2o-3
<br>Message me @cranella

For H2O REST API details and schema objects used by this client see:
<br>http://docs.h2o.ai/h2o/latest-stable/h2o-docs/rest-api-reference.html
