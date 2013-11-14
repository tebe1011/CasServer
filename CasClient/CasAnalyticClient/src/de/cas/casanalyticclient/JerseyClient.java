package de.cas.casanalyticclient;

import java.io.Serializable;
import java.util.ArrayList;

import javax.ws.rs.Path;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import com.sun.jersey.api.client.filter.HTTPBasicAuthFilter;
import com.sun.jersey.api.client.filter.LoggingFilter;
import com.sun.jersey.core.util.Base64;
import com.sun.jersey.core.util.MultivaluedMapImpl;
import com.sun.jersey.server.impl.uri.UriHelper;

public class JerseyClient implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	static final String URIStep1 = "http://172.24.60.216:8080/CasAnalyticServer/Extract/Step1";
	static final String URIStep2 = "http://172.24.60.216:8080/CasAnalyticServer/Extract/Step2";
	static final String URIStep3 = "http://172.24.60.216:8080/CasAnalyticServer/Extract/Step3";
	static final String URIStep4 = "http://172.24.60.216:8080/CasAnalyticServer/Extract/Step4";
	static final String URIStep5 = "http://172.24.60.216:8080/CasAnalyticServer/Extract/Step5";

	private WebResource webResource;
	private Client client;

	public JerseyClient() {

		ClientConfig config = new DefaultClientConfig();
		client = Client.create(config);
		client.addFilter(new HTTPBasicAuthFilter("SysAdmin", "sudo"));
	}

	public String doGetRequestStep1() {
		webResource = client.resource(URIStep1);
		ClientResponse clientresponse = webResource.get(ClientResponse.class);
		return clientresponse.getEntity(String.class);
	}

	public String doGetRequestStep2() {
		webResource = client.resource(URIStep2);
		ClientResponse clientresponse = webResource.get(ClientResponse.class);
		return clientresponse.getEntity(String.class);
	}
	
	public String doGetRequestStep3() {
		webResource = client.resource(URIStep3);
		ClientResponse clientresponse = webResource.get(ClientResponse.class);
		return clientresponse.getEntity(String.class);
	}
	
	public String doGetRequestStep4(JSONObject value) {
		webResource = client.resource(URIStep4);
		ClientResponse clientresponse = webResource.post(ClientResponse.class, value);
		return "";
	}

	public WebResource getWebResource() {
		return webResource;
	}

	public void setWebResource(WebResource webResource) {
		this.webResource = webResource;
	}
}
