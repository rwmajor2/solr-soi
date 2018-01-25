# solr-soi
ArcGIS Server Server Object Interceptor (SOI) talking to SOLR REST

This SOI intercepts calls to a dummy Map Service and querys a SOLR REST to retrieve data.  

The SOI (once enabled for a Map Service) depends on a solrconfig.json being present in a particular location for that Map Server.  That location is C:\arcgisserver\directories\arcgisoutput\SampleWorldCities_MapServer (for example).

This SOI also incorporates using a PKI certificate to talk to a PKI protected SOLR instance.  The soi.jks (in this case) would need to be located in that same directory.

This was developed using Eclipse and the ArcObjects Eclipse Plugin for ease of compiling the .soe file.  ***Note:  Althought this is an SOI, it still compiles it to an .soe file extension.***
