package com.esri.arcgis;

/*
 COPYRIGHT 1995-2014 ESRI
 TRADE SECRETS: ESRI PROPRIETARY AND CONFIDENTIAL
 Unpublished material - all rights reserved under the 
 Copyright Laws of the United States and applicable international
 laws, treaties, and conventions.

 For additional information, contact:
 Environmental Systems Research Institute, Inc.
 Attn: Contracts and Legal Services Department
 380 New York Street
 Redlands, California, 92373
 USA

 email: bmajor@esri.com
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.esri.arcgis.carto.MapServer;
import com.esri.arcgis.interop.AutomationException;
import com.esri.arcgis.interop.extn.ArcGISExtension;
import com.esri.arcgis.interop.extn.ServerObjectExtProperties;
import com.esri.arcgis.server.IServerObject;
import com.esri.arcgis.server.IServerObjectExtension;
import com.esri.arcgis.server.IServerObjectHelper;
import com.esri.arcgis.server.SOIHelper;
import com.esri.arcgis.server.json.JSONArray;
import com.esri.arcgis.server.json.JSONObject;
import com.esri.arcgis.system.ILog;
import com.esri.arcgis.system.IRESTRequestHandler;
import com.esri.arcgis.system.IRequestHandler;
import com.esri.arcgis.system.IRequestHandler2;
import com.esri.arcgis.system.IWebRequestHandler;
//import com.esri.arcgis.system.IWebRequestHandlerProxy;
import com.esri.arcgis.system.ServerUtilities;

import java.security.KeyStore;
import java.text.SimpleDateFormat;
/*
 * For an SOE to act as in interceptor, it needs to implement all request handler interfaces
 * IRESTRequestHandler, IWebRequestHandler, IRequestHandler2, IRequestHandler now the SOE/SOI can
 * intercept all types of calls to ArcObjects or custom SOEs.
 * 
 * This sample SOI can be used as the starting point to writing new SOIs. It is a basic example
 * which implements all request handlers and logs calls to ArcObjects or custom SOEs.
 */









import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

/*
 * For an interceptor you need to set additional properties for @ServerObjectExtProperties the annotation.
 * 1. interceptor = true, is used to identify an SOI
 * 2. servicetype = MapService | ImageService, can be used to assign an interceptor to an Image or Map Service.
 */
@ArcGISExtension
@ServerObjectExtProperties(displayName = "SOLR SOI", description = "SOLR SOI for query Big Data Stores", interceptor = true, servicetype = "MapService")
public class SOLRSOI implements IServerObjectExtension, IRESTRequestHandler,
		IWebRequestHandler, IRequestHandler2, IRequestHandler {
	private static final long serialVersionUID = 1L;
	private static final String ARCGISHOME_ENV = "AGSSERVER";
	private ILog serverLog;
	private IServerObject so;
	private SOIHelper soiHelper;

	/*
	 * JSON object used to store config information read in from a file specific to a Map SErvice.
	 */
	private JSONObject ConfigMap = null;
	private String mapsvcOutputDir = null;
	private SSLContext sslcontext = null;
	Map<String, String> fieldsearch = null;
	  
	/**
	 * Default constructor.
	 *
	 * @throws Exception
	 */
	public SOLRSOI() throws Exception {
		super();
	}

	/**
	 * init() is called once, when the instance of the SOE/SOI is created.
	 *
	 * @param soh the IServerObjectHelper
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws AutomationException the automation exception
	 */
	public void init(IServerObjectHelper soh) throws IOException,
			AutomationException {
		/*
		 * An SOE should retrieve a weak reference to the Server Object from the Server Object Helper in
		 * order to make any method calls on the Server Object and release the reference after making
		 * the method calls.
		 */
		// Get reference to server logger utility
		this.serverLog = ServerUtilities.getServerLogger();
		// Log message with server
		this.serverLog.addMessage(3, 200, "SOLR SOI Initialized "
				+ this.getClass().getName() + " SOI.");
		this.so = soh.getServerObject();
		String arcgisHome = getArcGISHomeDir();
		/* If null, throw an exception */
		if (arcgisHome == null) {
			serverLog.addMessage(1, 200,
					"Could not get ArcGIS home directory. Check if environment variable "
							+ ARCGISHOME_ENV + " is set.");
			throw new IOException(
					"Could not get ArcGIS home directory. Check if environment variable "
							+ ARCGISHOME_ENV + " is set.");
		}
		if (arcgisHome != null && !arcgisHome.endsWith(File.separator))
			arcgisHome += File.separator;
		
		// Load the SOI helper.    
		this.soiHelper = new SOIHelper(arcgisHome + "XmlSchema"	+ File.separator + "MapServer.wsdl");
		
		// Read in the SOLR config file for this particular service.
		getConfigFromFile(this.so);
		sslcontext = createSSLContext();
	}

	/**
	 * This method is called to handle REST requests.
	 *
	 * @param capabilities the capabilities
	 * @param resourceName the resource name
	 * @param operationName the operation name
	 * @param operationInput the operation input
	 * @param outputFormat the output format
	 * @param requestProperties the request properties
	 * @param responseProperties the response properties
	 * @return the response as byte[]
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws AutomationException the automation exception
	 */
	@Override
	public byte[] handleRESTRequest(String capabilities, String resourceName,
			String operationName, String operationInput, String outputFormat,
			String requestProperties, String[] responseProperties)
			throws IOException, AutomationException {
		/*
		 * Log message with server. Here we are logging who made the request,
		 * what operation was invoked and with what input parameters.
		 * 
		 * You can use different log codes to set up different log levels.
		 * 
		 * For example:
		 * Use log code 1 for Severe Messages.
		 * Use log code 2 for Warning Messages.
		 * Use log code 3 for Info Messages.
		 * Use log code 4 for Fine Messages.
		 * Use log code 100 for Debug Messages.
		 * 
		 * Note: You can also use the ILog interface to get more information on log message levels.
		 */
		
		serverLog
				.addMessage(3, 200, "Big Data SOLR SOI. User: "
						+ getLoggedInUserName() + ", Capabilities: "
						+ capabilities + ", resourceName: "
						+ resourceName + ", Operation: "
						+ operationName + ", Operation Input: "
						+ operationInput + ", outputFormat: " 
						+ outputFormat + ", requestProperties: "
						+ requestProperties);
						//+ processOperationInput(operationInput));
		
		/*
		 * Add code to manipulate REST requests here
		 */

		// Find the correct delegate to forward the request too
		IRESTRequestHandler restRequestHandler = soiHelper.findRestRequestHandlerDelegate(so);
		if (restRequestHandler != null) {
			
			// If the config file for the SOLR implementation didn't get read properly or does
			//   not exist, then don't do any SOI'ing.  Just let the request pass all the way through
			//   to the underlying service.  An ArcGIS Server admin will need to look at the Logs in
			//   ArcGIS Server Manager to troubleshoot why this file wasn't read properly
			if (ConfigMap == null)
			{
				serverLog.addMessage(3, 200, "SOLR SOI:  ConfigMap not properly initialized.  Passing through.");
				return restRequestHandler.handleRESTRequest(capabilities,
						resourceName, operationName, operationInput, outputFormat,
						requestProperties, responseProperties);
			}
			
			// if the operationName is query, then this be interacting with a FeatureServer
			if ((operationName.equalsIgnoreCase("query")) && (resourceName.equalsIgnoreCase("layers/0")))
			{
				//
				// Get various JSON objects from Query request to help determine the type of request.
				//   3 types:  Draw, Identify, and Export Data.
				//
				String fields = new JSONObject(operationInput).getString("outFields");
				String[] reqFields = fields.split(",");  
				long numRequestedReturnFields = reqFields.length;   // Number of fields requested is the only way to distinguish between an Identify and Draw
				
				boolean returnGeom = new JSONObject(operationInput).getBoolean("returnGeometry");
				boolean hasGeom = new JSONObject(operationInput.toString()).has("geometry");
				boolean hasWhere = new JSONObject(operationInput.toString()).has("where");
				boolean hasObjectids = new JSONObject(operationInput.toString()).has("objectIds");  // This means a Select
				boolean hasOneEqualsOne = false;
				
				// Map used to hold various field searches that come in; used later in building the SOLR Query
				String[] fc_fields = ConfigMap.getString("fc_fields").split(",");  // Get field names for FC from solrconfig.json file
				fieldsearch = new HashMap<String, String>();
				String bucket_text = "";
				String dtrange = null;
				
				// 2013-01-01T00:00:00Z TO 2015-12-31T00:00:00Z
				if (hasWhere){
					String whereclause = new JSONObject(operationInput).getString("where");
					String[] where_pieces = whereclause.split("(?i)and");
					for (int i = 0; i < where_pieces.length; i++)
					{
						String[] parts2 = where_pieces[i].split(">=|<=|=|>|<|(?i)like");
						if (parts2[0].toLowerCase().contains(ConfigMap.getString("textsearchfield").toLowerCase()))
						{
							bucket_text = parts2[1].toLowerCase().trim().replaceAll("\\P{Alnum}", "");;
							serverLog.addMessage(3, 200, "SOLR SOI:  Bucket text field set to " + bucket_text);
						}
						else if (parts2[0].toLowerCase().contains(ConfigMap.getString("fcdatefield").toLowerCase()))
						{
							// *** Made changes to string parsing per Dale's email on Sept. 17th
							Pattern pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})");
							Matcher matcher = pattern.matcher(parts2[1].trim());
							while (matcher.find())
							{
								  if (dtrange == null){
									  String[] temp = matcher.group().toString().split(" ");
									  dtrange = temp[0] + "T" + temp[1] + "Z TO ";
								  }
								  else
								  {
									  String[] temp = matcher.group().toString().split(" ");
									  dtrange += temp[0] + "T" + temp[1] + "Z";
								  }
							  }
							//dtrange = vals[1].trim().replace("'", "");
							serverLog.addMessage(3, 200, "SOLR SOI:  Date Range set to " + dtrange);
						}
						else if (parts2[0].toLowerCase().equalsIgnoreCase("1") && parts2[1].toLowerCase().equalsIgnoreCase("1"))
						{
							hasOneEqualsOne = true;
							serverLog.addMessage(3, 200, "SOLR SOI:  hasOneEqualsOne set to true.");
						}
						else
						{
							// see if any other field name searches where passed in
							String fld = parts2[0].toLowerCase().trim().replaceAll("[^\\w_]+", "");   //replace all non-alpha except _
							serverLog.addMessage(3, 200, "SOLR SOI:  field query passed in: " + fld);
							for (int k = 0; k < fc_fields.length; k++)
							{
								serverLog.addMessage(4, 200, "SOLR SOI:  comparing " + fld + " to " + fc_fields[k].toLowerCase());
								if (fld.equalsIgnoreCase(fc_fields[k].toLowerCase()))
								{
									// Not sure about replacing any non-alpha character in the search criteria as it may degrade the criteria
									//   Only stripping  ' ) % "  for right now
									String searchstr = parts2[1].toLowerCase().trim().replace("'", "").replace(")", "").replace("%", "").replace("\"", "");
									fieldsearch.put(fld, searchstr);
									serverLog.addMessage(3, 200, "SOLR SOI:  Added additional field query of " + fld + " : " + searchstr);
								}
							}
						}
					}
				}
				
				//  Per requirement from Dale, if no Date is passed in on the Definition Query, then we
				//    don't support this.  Passing in a date is required for interacting with SOLR to 
				//    help limit the scope of queries for performance.  In this case, no SOI'ing is done
				//    and the request is passed through to the underlying service and Featureclass.
				if (dtrange == null)
				{
					serverLog.addMessage(3, 200, "SOLR SOI:  No Date Range or an invalid date range was provided.  Passing request through.");
					return restRequestHandler.handleRESTRequest(capabilities,
							resourceName, operationName, operationInput, outputFormat,
							requestProperties, responseProperties);
				}
								
				// set up some variables that get used in the various condition statements.
				//URL u = null;
			    //InputStream is = null;
			    JSONObject response_new = getJSONResponse();  // the new response that will get sent back to the client
				
				
				/*
				 *  Handle a Draw request on a Feature Service from ArcMap.
				 *  This is a unique set of properties where it only requests the objectid field, 
				 *  requests geometries to be returned, has an extent geometry passed in, and
				 *  does _NOT_ have a JSON key called 'objectIds'.
				 * 
				 * Why do I have "(numRequestedReturnFields <= 3)"?  The reason for this is in draw
				 *   requests, only the minimum fields are returned.  I am assuming that only objectid, 
				 *   bucket, and date_created will only ever be called in a simple draw request.  The
				 *   reason bucket and date_created would be returned is because they would be part of
				 *   the where clause and ArcGIS Server returns all fields part of the where clause
				 *   plus the objectid.
				 */
				if ((numRequestedReturnFields <= ConfigMap.getInt("maxfieldcount_for_defquery")) && (!(fields.equalsIgnoreCase("*")))&& (returnGeom) && (hasGeom) && (!(hasObjectids)))
				{					
					serverLog.addMessage(3, 200, "SOLR SOI:  This is a simple draw with an extent passed in.");
					
					Object aObj = new JSONObject(operationInput).get("geometry");
					double[] dblArray = null;
					 
					// Test if geometry string is coming from ArcMap
					if (aObj instanceof String) {
						String strcoords = new JSONObject(operationInput).getString("geometry");
						String[] strArray = strcoords.split(",");
				        dblArray = new double[strArray.length];
				        for(int i = 0; i < strArray.length; i++) {
				        	dblArray[i] = Double.parseDouble(strArray[i]);
				        }
					}
					// else test if it is coming from JSAPI
					else if (aObj instanceof JSONObject)
					{
				        dblArray = new double[4];
				        dblArray[0] = ((JSONObject) aObj).getDouble("ymin");
				        dblArray[1] = ((JSONObject) aObj).getDouble("xmin");
				        dblArray[2] = ((JSONObject) aObj).getDouble("ymax");
				        dblArray[3] = ((JSONObject) aObj).getDouble("xmax");
					}
			        			        
				    try 
				    {
				        
				    	String jsonresponse = querySOLR(bucket_text, dtrange, true, dblArray[1], dblArray[0], dblArray[3], dblArray[2], 0);
				    	
				        JSONArray featuresArray = new JSONArray();
				        JSONObject recvObj;
						try
						{
							recvObj = new JSONObject(jsonresponse);
							
							JSONObject numrecords = recvObj.getJSONObject(ConfigMap.getString("solrresponsetag"));
							JSONArray feats = (JSONArray)numrecords.get(ConfigMap.getString("solrdocstag"));

						    for (int i = 0; i < feats.length(); i++)
						    {
						    	JSONObject feat = feats.getJSONObject(i);  // Get the JSON feature from the SOLR query
						    	
						    	// Accommodate differences between the point type returned between Esri environment and ATI environment
						    	String[] coords;
						    	if (ConfigMap.getInt("returnpointtype") == 0)
						    	{
						    		JSONArray x = (JSONArray)feat.get(ConfigMap.getString("ptgeofield"));
									coords = x.get(0).toString().split(",");  
						    	}
						    	else
						    	{
						    		coords = feat.getString(ConfigMap.getString("ptgeofield")).split(",");
						    	}
						    	JSONObject featJSON = new JSONObject();  // Create new JSON feature to put into the response
								featJSON.put("geometry",  new JSONObject().put("x", coords[1]).put("y", coords[0]));
								featJSON.put("attributes",  new JSONObject().put("objectid", i));
								featuresArray.put(i, featJSON);
							}
						}
						catch (Exception ex) 
						{
							serverLog.addMessage(3, 200, "SOLR SOI:  --- error occurred.");
							ex.printStackTrace();
						}
						
				        response_new.put("features", featuresArray);
				        
				    } catch (Exception ignore) {
				    	serverLog.addMessage(3, 200, "SOLR SOI:  --- error occurred with exception ignore.");
				    	serverLog.addMessage(3, 200, ignore.getMessage());
				    }
				    
				    serverLog.addMessage(4, 200, "SOLR SOI DRAW Response:  " + response_new.toString());
				    return response_new.toString().getBytes("UTF-8");
				}
				
				/*
				 *  Handle a Identify request on a Feature Service.
				 *  This is a unique set of properties where it requests all fields of information (not just objectid), 
				 *  requests geometries to be returned, has an extent geometry passed in.
				 * 
				 */
				else if (((numRequestedReturnFields > ConfigMap.getInt("maxfieldcount_for_defquery")) || (fields.equalsIgnoreCase("*"))) && (returnGeom) && (hasGeom))
				{
					serverLog.addMessage(3, 200, "SOLR SOI:  This is an Identify request, an Export within View Extent, or a Draw request from Portal since it always wants all attributes back.");
					
					Object aObj = new JSONObject(operationInput).get("geometry");
					double[] dblArray = null;
					 
					// Test if geometry string is coming from ArcMap
					if (aObj instanceof String) {
						String strcoords = new JSONObject(operationInput).getString("geometry");
						String[] strArray = strcoords.split(",");
				        dblArray = new double[strArray.length];
				        for(int i = 0; i < strArray.length; i++) {
				        	dblArray[i] = Double.parseDouble(strArray[i]);
				        }
					}
					// else test if it is coming from JSAPI
					else if (aObj instanceof JSONObject)
					{
				        dblArray = new double[4];
				        dblArray[0] = ((JSONObject) aObj).getDouble("ymin");
				        dblArray[1] = ((JSONObject) aObj).getDouble("xmin");
				        dblArray[2] = ((JSONObject) aObj).getDouble("ymax");
				        dblArray[3] = ((JSONObject) aObj).getDouble("xmax");
					}
			         
			        response_new.put("fields", getJSONFields(false));
	
				    try 
				    {
				        
				    	String jsonresponse = querySOLR(bucket_text, dtrange, true, dblArray[1], dblArray[0], dblArray[3], dblArray[2], 1);
				    	
				        JSONArray featuresArray = new JSONArray();
				        JSONObject recvObj;
						try
						{
							recvObj = new JSONObject(jsonresponse);
							JSONObject numrecords = recvObj.getJSONObject(ConfigMap.getString("solrresponsetag"));
							JSONArray feats = (JSONArray)numrecords.get(ConfigMap.getString("solrdocstag"));

						    for (int i = 0; i < feats.length(); i++)
						    {
						    	JSONObject feat = feats.getJSONObject(i);  // Get the JSON feature from the SOLR query
						    	
						    	// Accommodate differences between the point type returned between Esri environment and ATI environment
						    	String[] coords;
						    	if (ConfigMap.getInt("returnpointtype") == 0)
						    	{
						    		JSONArray x = (JSONArray)feat.get(ConfigMap.getString("ptgeofield"));
									coords = x.get(0).toString().split(",");  
						    	}
						    	else
						    	{
						    		coords = feat.getString(ConfigMap.getString("ptgeofield")).split(",");
						    	}
						    	JSONObject featJSON = new JSONObject();  // Create new JSON feature to put into the response
								featJSON.put("geometry",  new JSONObject().put("x", coords[1]).put("y", coords[0]));
								String tweet = feat.getString(ConfigMap.getString("solrtextsearchfield"));
								tweet = tweet.replaceAll("\\P{Print}","");
								
								JSONObject att = new JSONObject();
								att.put("objectid", i);
								
								JSONArray mappings = ConfigMap.getJSONArray("fc_to_solr_field_mappings");
								for (int j = 0; j < mappings.length(); j++)
								{
									JSONObject map = mappings.getJSONObject(j);
									String key = map.keys().next().toString();
									if (key.equalsIgnoreCase(ConfigMap.getString("fcdatefield")))
									{
										String str = feat.getString(map.getString(key)).replace("T", " ").replace("Z", " ");							
									    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
									    Date date = df.parse(str);
									    long epoch = date.getTime();
									    att.put(key.toLowerCase(), epoch);
									}
									else
									{
										att.put(key.toLowerCase(), feat.getString(map.getString(key)));
									}
								}
								att.put("point_x", coords[1]);
								att.put("point_y", coords[0]);
								att.put("bucket", bucket_text);
								
								featJSON.put("attributes", att);
								featuresArray.put(i, featJSON);
							}
						}
						catch (Exception ex) 
						{
							serverLog.addMessage(3, 200, "SOLR SOI:  --- error occurred.");
							ex.printStackTrace();
						}
						
				        response_new.put("features", featuresArray);
				        
				    } catch (Exception ignore) {
				    	serverLog.addMessage(3, 200, "SOLR SOI:  --- error occurred with exception ignore.");
				    	serverLog.addMessage(3, 200, ignore.getMessage());
				    }
				    
				    serverLog.addMessage(4, 200, "SOLR SOI IDENTIFY Response:  " + response_new.toString());
				    return response_new.toString().getBytes("UTF-8");
				}
				
				/*
				 *  Handle a Export Data (second) request on a Feature Service.
				 *  This is a unique set of properties where it requests all fields of information (not just objectid), 
				 *  has a where clause, requests geometries to be returned, and
				 *  does _NOT_ have an extent geometry passed in.
				 * (numRequestedReturnFields <= 3)
				 */
				//else if (!(fields.equalsIgnoreCase("objectid")) && (hasWhere) && (!(hasGeom)) && (returnGeom))
				else if ((numRequestedReturnFields > ConfigMap.getInt("maxfieldcount_for_defquery")) && (hasWhere) && (!(hasGeom)) && (returnGeom))
				{
					
					serverLog.addMessage(3, 200, "SOI action: Second call for Export Data.");
								         
					response_new.put("fields", getJSONFields(false));
	
				    try {
				    	String jsonresponse = querySOLR(bucket_text, dtrange, false, 0, 0, 0, 0, 1);
				    	
				        JSONArray featuresArray = new JSONArray();
				        JSONObject recvObj;
						try
						{
							recvObj = new JSONObject(jsonresponse);
							JSONObject numrecords = recvObj.getJSONObject(ConfigMap.getString("solrresponsetag"));
							JSONArray feats = (JSONArray)numrecords.get(ConfigMap.getString("solrdocstag"));

						    for (int i = 0; i < feats.length(); i++)
						    {
						    	JSONObject feat = feats.getJSONObject(i);  // Get the JSON feature from the SOLR query
						    	
						    	// Accommodate differences between the point type returned between Esri environment and ATI environment
						    	String[] coords;
						    	if (ConfigMap.getInt("returnpointtype") == 0)
						    	{
						    		JSONArray x = (JSONArray)feat.get(ConfigMap.getString("ptgeofield"));
									coords = x.get(0).toString().split(",");  
						    	}
						    	else
						    	{
						    		coords = feat.getString(ConfigMap.getString("ptgeofield")).split(",");
						    	}
						    	JSONObject featJSON = new JSONObject();  // Create new JSON feature to put into the response
								featJSON.put("geometry",  new JSONObject().put("x", coords[1]).put("y", coords[0]));
								String tweet = feat.getString(ConfigMap.getString("solrtextsearchfield"));
								tweet = tweet.replaceAll("\\P{Print}","");
								
								JSONObject att = new JSONObject();
								att.put("objectid", i);
								
								JSONArray mappings = ConfigMap.getJSONArray("fc_to_solr_field_mappings");
								for (int j = 0; j < mappings.length(); j++)
								{
									JSONObject map = mappings.getJSONObject(j);
									String key = map.keys().next().toString();
									if (key.equalsIgnoreCase(ConfigMap.getString("fcdatefield")))
									{
										String str = feat.getString(map.getString(key)).replace("T", " ").replace("Z", " ");							
									    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
									    Date date = df.parse(str);
									    long epoch = date.getTime();
									    att.put(key.toLowerCase(), epoch);
									}
									else
									{
										att.put(key.toLowerCase(), feat.getString(map.getString(key)));
									}
								}
								att.put("point_x", coords[1]);
								att.put("point_y", coords[0]);
								att.put("bucket", bucket_text);
								
								featJSON.put("attributes", att);
								featuresArray.put(i, featJSON);
							}
						}
						catch (Exception ex) 
						{
							serverLog.addMessage(3, 200, "--- error occurred.");
							ex.printStackTrace();
						}
						
				        response_new.put("features", featuresArray);
				    	
				    } catch (MalformedURLException mue) {
				    	System.out.println("Ouch - a MalformedURLException happened.");
			         	mue.printStackTrace();
			         	System.exit(1);
				    } catch (IOException ioe) {
				    	System.out.println("Oops- an IOException happened.");
				    	ioe.printStackTrace();
			         	System.exit(1);
				    } 
				    
				    serverLog.addMessage(4, 200, "SOLR SOI EXPORT DATA Response:  " + response_new.toString());
				    return response_new.toString().getBytes("UTF-8");
				}
				//}
				else
				{
					return restRequestHandler.handleRESTRequest(capabilities,
							resourceName, operationName, operationInput, outputFormat,
							requestProperties, responseProperties);
				}
				//serverLog.addMessage(3, 200, "SOI  Response:  " + new String(response));
				//return response;
			}
			
			// not a query call, so just pass on through and let return
			else {
				return restRequestHandler.handleRESTRequest(capabilities,
						resourceName, operationName, operationInput, outputFormat,
						requestProperties, responseProperties);
			}
		}

		return null;
	}

	/**
	 * This method is called to handle SOAP requests.
	 *
	 * @param capabilities the capabilities
	 * @param request the request
	 * @return the response as String
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws AutomationException the automation exception
	 */
	@Override
	public String handleStringRequest(String capabilities, String request)
			throws IOException, AutomationException {
		// Log message with server
		serverLog.addMessage(3, 200, "Request received in Sample Object Interceptor for handleStringRequest");

		/*
		 * Add code to manipulate SOAP requests here
		 */

		// Find the correct delegate to forward the request too
		IRequestHandler requestHandler = soiHelper
				.findRequestHandlerDelegate(so);
		if (requestHandler != null) {
			// Return the response
			return requestHandler.handleStringRequest(capabilities, request);
		}

		return null;
	}

	/**
	 * This method is called by SOAP handler to handle OGC requests.
	 *
	 * @param httpMethod
	 * @param requestURL the request URL
	 * @param queryString the query string
	 * @param capabilities the capabilities
	 * @param requestData the request data
	 * @param responseContentType the response content type
	 * @param respDataType the response data type
	 * @return the response as byte[]
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws AutomationException the automation exception
	 */
	@Override
	public byte[] handleStringWebRequest(int httpMethod, String requestURL,
			String queryString, String capabilities, String requestData,
			String[] responseContentType, int[] respDataType)
			throws IOException, AutomationException {
		serverLog.addMessage(3, 200, "Request received in Sample Object Interceptor for handleStringWebRequest");

		/*
		 * Add code to manipulate OGC (WMS, WFC, WCS etc) requests here
		 */

		IWebRequestHandler webRequestHandler = soiHelper
				.findWebRequestHandlerDelegate(so);
		if (webRequestHandler != null) {
			return webRequestHandler.handleStringWebRequest(httpMethod,
					requestURL, queryString, capabilities, requestData,
					responseContentType, respDataType);
		}

		return null;
	}

	/**
	 * This method is called to handle binary requests from desktop.
	 *
	 * @param capabilities the capabilities
	 * @param request
	 * @return the response as byte[]
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws AutomationException the automation exception
	 */
	@Override
	public byte[] handleBinaryRequest2(String capabilities, byte[] request)
			throws IOException, AutomationException {
		serverLog
				.addMessage(3, 200,
						"Request received in Sample Object Interceptor for handleBinaryRequest2");

		/*
		 * Add code to manipulate Binary requests from desktop here
		 */

		IRequestHandler2 requestHandler = soiHelper
				.findRequestHandler2Delegate(so);
		if (requestHandler != null) {
			return requestHandler.handleBinaryRequest2(capabilities, request);
		}

		return null;
	}

	/**
	 * Build a SOLR Query and retrieve the response.
	 * 
	 * @return
	 * @throws IOException 
	 * @throws AutomationException 
	 */
	private String querySOLR(String btext, String dt_range, boolean useExtent, double latMin,
								double longMin, double latMax, double longMax, long type) 
				throws AutomationException, IOException {
		
		URL u = null;
	    //InputStream is = null;
		
        String queryStr = "&q=*:*";
        if (!btext.isEmpty())
        {
        	queryStr += "&fq=bucket:" + btext;
        }
        if (!fieldsearch.isEmpty())
        {
        	// Add additional search criteria for any field searches passed in besides bucket
        	for(String key: fieldsearch.keySet())
        	{
       
        		JSONArray mappings = ConfigMap.getJSONArray("fc_to_solr_field_mappings");
				for (int j = 0; j < mappings.length(); j++)
				{
					JSONObject map = mappings.getJSONObject(j);
					String key2 = map.keys().next().toString();
					if (key2.equalsIgnoreCase(key))
					{
						// *** Made changes to URL Encode values per Dale's email on Sept. 17th
						queryStr += "&fq=" + map.getString(key2) + ":" + URLEncoder.encode(fieldsearch.get(key), "UTF-8");
					}
				}
        	}
        }
        if (!dt_range.isEmpty())
        {
        	queryStr += "&fq={!cache=false}" + ConfigMap.getString("solrdatefield") + ":[" + dt_range + "]";
        }
        if (useExtent)
        {
        	queryStr += "&fq={!cache=false}" + ConfigMap.getString("ptgeofield") + ":[" + Math.max(-90.0, latMin) + "," + Math.max(-180, longMin) + " TO " + 
            		Math.min(90.0, latMax) + "," + Math.min(180.0, longMax) + "]";
        }
        
        if (type == 0)
        {
        	// Draw; only return geometries
        	queryStr += "&rows=" + ConfigMap.getLong("maxrows") + "&fl=" + ConfigMap.getString("ptgeofield") + 
        			"&wt=json&timeAllowed=" + ConfigMap.getLong("maxwaittime");
        }
        else if (type == 1)
        {
        	// Identify; returning everything but tweet_text right now for unicode purposes
        	queryStr += "&rows=" + ConfigMap.getLong("maxrows") + "&fl=" + ConfigMap.getString("solrsearchfields") +
                     "&wt=json&timeAllowed=" + ConfigMap.getLong("maxwaittime");
        }
        else
        	queryStr += "&rows=" + ConfigMap.getLong("maxrows") + "&fl=" + ConfigMap.getString("solrsearchfields") +
                    "&wt=json&timeAllowed=" + ConfigMap.getLong("maxwaittime");
        
        //serverLog.addMessage(3, 200, "SOLR Query:  http://hc1.esri.com:8983/solr/esri_collection_shard1_replica1/select/?" + queryStr);
        
        try
        {
	        URI testURI = null;
	        
			try {
				int port = ConfigMap.getInt("solrport");
				testURI = new URI(ConfigMap.getString("protocol"), null, ConfigMap.getString("solrserver"), port, ConfigMap.getString("solrselect"), queryStr, null);
			} catch (URISyntaxException e) {
				serverLog.addMessage(3, 200, "SOLR Query:  URISyntaxException occurred...");
				e.printStackTrace();
			}
	        
	        u = testURI.toURL();
   
        } catch (MalformedURLException mue) {
	    	//System.out.println("Ouch - a MalformedURLException happened.");
	    	mue.printStackTrace();
	    	serverLog.addMessage(3, 200, "SOLR Query:  MalformedURLException occurred...");
	    	return "";
	    } 
        
        serverLog.addMessage(3, 200, "SOLR Query:  " + ConfigMap.getString("protocol") + "://" + ConfigMap.getString("solrserver") + ":" + 
        							 ConfigMap.getInt("solrport") + ConfigMap.getString("solrselect") + "?" + queryStr);
        
	    try 
	    {
	    	HttpsURLConnection urlCon =(HttpsURLConnection)u.openConnection();
	    	((HttpsURLConnection)urlCon).setSSLSocketFactory(sslcontext.getSocketFactory());
	    	
	    	InputStream input = urlCon.getInputStream();
	        //is = u.openStream();         // throws an IOException
	        BufferedReader d = new BufferedReader(new InputStreamReader(input, "UTF-8"));
	        String jsonresponse = "";
	        String inputLine;
			while ((inputLine = d.readLine()) != null)
			{
			      jsonresponse += inputLine;
			}
			
			// Removing any 'u's from the response because that results in invalid JSON
			//jsonresponse = jsonresponse.replaceAll(":u'", ":'");
			d.close();
			
			// serverLog.addMessage(3, 200, "SOLR Query:  returning json response from solr...");
			serverLog.addMessage(4, 200, "---------- SOLR Query jsonresponse:  " + jsonresponse);
			return jsonresponse;
	    }
	    catch (IOException ioe) {
	    	//System.out.println("Oops- an IOException happened.");
	    	//ioe.printStackTrace();
	    	serverLog.addMessage(3, 200, "SOLR Query:  Oops- an IOException happened...");
      		return "";
	    } 
	}
	
	
	/**
	 * Basic starter header of a JSON response for a Feature Service.
	 *   Assumptions:  Point Featureclass, 4326 Projection.  Probably should parameterize some.
	 * 
	 * @return
	 */
	private JSONObject getJSONResponse() {
		
		JSONObject response_new = new JSONObject();
		response_new.put("objectIdFieldName", "objectid");
		response_new.put("globalIdFieldName", "");
		response_new.put("geometryType", "esriGeometryPoint");
		response_new.put("spatialReference", new JSONObject().put("wkid", 4326).put("latestWkid", 4326));
		return response_new;
		
	}
	
	/**
	 * Return the basic JSON syntax for a Feature Service response.
	 *   It will either have just the objectid field (e.g. Draws) or all fields (read from Config file)
	 *   
	 * @return
	 */
	private JSONArray getJSONFields(boolean oidsonly) {
		
		JSONArray fldArray = new JSONArray();
		JSONObject fldJSON = new JSONObject();
		if (oidsonly) {
			fldJSON.put("name", "objectid");
			fldJSON.put("alias", "objectid");
			fldJSON.put("type", "esriFieldTypeOID");
			fldArray.put(0, fldJSON);
			return fldArray;
		}
		else {
			return ConfigMap.getJSONArray("fields");
		}

	}
	/**
	 * Return the logged in user's user name.
	 * 
	 * @return
	 */
	private String getLoggedInUserName() {
		try {
			/*
			 * Get the user information.
			 */
			String userName = ServerUtilities.getServerUserInfo().getName();

			if (userName.isEmpty()) {
				return new String("Anonymous User");
			}
			return userName;
		} catch (Exception ignore) {
		}

		return new String("Anonymous User");
	}

	/**
	 * Get bbox from operationInput
	 * 
	 * @param operationInput
	 * @return
	 */
//	private String processOperationInput(String operationInput) {
//		try {
//			return "bbox = " + new JSONObject(operationInput).getString("bbox");
//		} catch (Exception ignore) {
//		}
//		return new String("No input parameters");
//	}

	/**
	 * This method is called to handle schema requests for custom SOE's.
	 *
	 * @return the schema as String
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws AutomationException the automation exception
	 */
	@Override
	public String getSchema() throws IOException, AutomationException {
		serverLog.addMessage(3, 200,
				"Request received in Sample Object Interceptor for getSchema");

		/*
		 * Add code to manipulate schema requests here
		 */

		IRESTRequestHandler restRequestHandler = soiHelper
				.findRestRequestHandlerDelegate(so);
		if (restRequestHandler != null) {
			return restRequestHandler.getSchema();
		}

		return null;
	}

	/**
	 * This method is called to handle binary requests from desktop. It calls the
	 * <code>handleBinaryRequest2</code> method with capabilities equal to null.
	 *
	 * @param request
	 * @return the response as the byte[]
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws AutomationException the automation exception
	 */
	@Override
	public byte[] handleBinaryRequest(byte[] request) throws IOException,
			AutomationException {
		serverLog
				.addMessage(3, 200,
						"Request received in Sample Object Interceptor for handleBinaryRequest");

		/*
		 * Add code to manipulate Binary requests from desktop here
		 */

		IRequestHandler requestHandler = soiHelper
				.findRequestHandlerDelegate(so);
		if (requestHandler != null) {
			return requestHandler.handleBinaryRequest(request);
		}

		return null;
	}

	/**
	 * shutdown() is called once when the Server Object's context is being shut down and is about to
	 * go away.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws AutomationException the automation exception
	 */
	public void shutdown() throws IOException, AutomationException {
		/*
		 * The SOE should release its reference on the Server Object Helper.
		 */
		this.serverLog.addMessage(3, 200, "Shutting down "
				+ this.getClass().getName() + " SOI.");
		this.serverLog = null;
		this.so = null;
		this.soiHelper = null;
	}

	/**
	 * Returns the ArcGIS home directory path.
	 * 
	 * @return
	 * @throws Exception
	 */
	private String getArcGISHomeDir() throws IOException {
		String arcgisHome = null;
		/* Not found in env, check system property */
		if (System.getProperty(ARCGISHOME_ENV) != null) {
			arcgisHome = System.getProperty(ARCGISHOME_ENV);
		}
		if (arcgisHome == null) {
			/* To make env lookup case insensitive */
			Map<String, String> envs = System.getenv();
			for (String envName : envs.keySet()) {
				if (envName.equalsIgnoreCase(ARCGISHOME_ENV)) {
					arcgisHome = envs.get(envName);
				}
			}
		}
		if (arcgisHome != null && !arcgisHome.endsWith(File.separator)) {
			arcgisHome += File.separator;
		}
		return arcgisHome;
	}
	
	 /**
	   * Reads a permission file and return the defined permissions.
	   * 
	   * @param serverobject
	   * @throws IOException
	   */
	  private void getConfigFromFile(IServerObject serverobject) throws IOException {
	    //String serverDir = null;
	    MapServer mapserver= (MapServer)serverobject;
	    String physicalOutputDir= mapserver.getPhysicalOutputDirectory();
	    int index = physicalOutputDir.indexOf(File.separator + "directories" + File.separator + "arcgisoutput");
	    if(index > 0) {
	      serverLog.addMessage(4, 200, "SOLR SOI: The physical directory for output files: " + physicalOutputDir);
	      //serverDir = physicalOutputDir.substring(0,index);
	    } 
	    else {
	      serverLog.addMessage(1, 200,"SOLR SOI: Incorrect physical directory for output files: " + physicalOutputDir);
	      throw new IOException("Incorrect physical directory for output files: " + physicalOutputDir);   
	    }
	    /*
	     * Permission are read from this external file. Advantage of an external file is that same SOI can
	     * be used for multiple services and permission for all of these services is read from the
	     * permission.json file.
	     */
	    String permssionFilePath = physicalOutputDir + File.separator +  "solrconfig.json";
	    // Read the permissions file
	    if (new File(permssionFilePath).exists()) {
	      serverLog.addMessage(3, 200, "SOLR SOI: The SOLR config file is located at : " + permssionFilePath);
	      mapsvcOutputDir = physicalOutputDir;
	      ConfigMap = readConfigFile(permssionFilePath);
	    } else {
	      serverLog.addMessage(1, 200,"SOLR SOI: Cannot find the SOLR Config file at " + permssionFilePath);
	      throw new IOException("Cannot find the SOLR Config file at " + permssionFilePath);   
	    }
	  }
	  
	  /**
	   * Read config information from disk
	   * 
	   * @param fileName path and name of the file to read permissions from
	   * @return
	 * @throws IOException 
	 * @throws AutomationException 
	   */
	  private JSONObject readConfigFile(String fileName) throws AutomationException, IOException {
	    // read the permissions file
	    BufferedReader reader;
	    JSONObject solrconfig = null;
	    try {
	      reader = new BufferedReader(new FileReader(fileName));
	      String line = null;
	      String permissionFileDataString = "";
	      while ((line = reader.readLine()) != null) {
	        permissionFileDataString += line;
	      }
	      serverLog.addMessage(4, 200, "SOLR SOI:  SOLR Config File Read : " + permissionFileDataString);
	      solrconfig = new JSONObject(permissionFileDataString);
	      reader.close();
	    } catch (Exception ignore) {
	    	serverLog.addMessage(2, 200, "SOLR SOI:  Error occurred in readConfigFile, Exception ignore");
	    }
	    return solrconfig;
	  }
	  
	  private SSLContext createSSLContext() throws AutomationException, IOException
	  {
		  try
		  {
			  KeyStore keyStore = KeyStore.getInstance("JKS");
			  FileInputStream fis = new FileInputStream(mapsvcOutputDir + File.separator + ConfigMap.getString("keystore"));
			  keyStore.load(fis, ConfigMap.getString("keystorepassword").toCharArray());  // password
		
			  KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
			  kmf.init(keyStore, ConfigMap.getString("keystorepassword").toCharArray());
			  KeyManager[] keyManagers = kmf.getKeyManagers();
			 
			  TrustManager[] trustStoreManagers=null;
			  KeyStore trustedCertStore=KeyStore.getInstance("JKS");
	
			  InputStream tsStream= new FileInputStream(mapsvcOutputDir + File.separator + ConfigMap.getString("keystore"));
			  trustedCertStore.load(tsStream, ConfigMap.getString("keystorepassword").toCharArray());
			  TrustManagerFactory tmf=TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
			  tmf.init(trustedCertStore);
			  trustStoreManagers=tmf.getTrustManagers();
			  
			  SSLContext sslContext = SSLContext.getInstance("TLS");
			  sslContext.init(keyManagers, trustStoreManagers, null);  // create the sslContext of user certs and trusted certs
			  
			  return sslContext;
		  }
		  catch (Exception ignore) {
	    	serverLog.addMessage(2, 200, "SOLR SOI:  The TrustManager could not be established.  Returning null.");
	    	return null;
		  }
	  }
}