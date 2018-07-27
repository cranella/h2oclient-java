# h2oclient-java
Java REST API client for the H2O machine learning platform.  In addition to building, training, and downloading machine learning models 
in H2O, this library facilitates the use of Rapids expressions for data frame manipulation using the methods provided in the H2OFrame 
class. 

For API details and schema objects used by this client see:
http://docs.h2o.ai/h2o/latest-stable/h2o-docs/rest-api-reference.html

Developed for AT&T by Chris Ranella, July 2018

# Requirements
h2o version 3.2.x <br>
See latest release here: http://h2o-release.s3.amazonaws.com/h2o/rel-slater/3/index.html 

# Installation
Build using maven and import jar into your Java project
<br>(https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html)

# Configuration
Use H2OConnection.newInstance(HttpUrl url) to connect to existing h2o cluster

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
Gitter H2O Developer Chat: https://gitter.im/h2oai/h2o-3
<br>Message me @cranella


Javadoc:
https://cranella.github.io/h2oclient-java/
