package de.cas.etl;

import java.util.ArrayList;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.codehaus.jettison.json.JSONObject;

import de.cas.db.Database;

@Path("/Extract")
public class JerseyServer {

	private QueryBuilder dictionarieBuilder = new QueryBuilder();
	private ConnectorJDBC jdbcConnectorMSSQL = new ConnectorJDBC();
	private Transform transform = new Transform();
	private Load loader = new Load();
	private RespondQuery respondQuery = new RespondQuery();
	private Logik logik = new Logik();
	
	@GET @Path("/Step1")
	@Produces(MediaType.TEXT_HTML)
	public String requestStep1(@Context UriInfo uriInfo, @Context HttpHeaders headers) {
		
		System.out.println("EXTRACT STARTED");
		
		dictionarieBuilder.createNameDictionarie(jdbcConnectorMSSQL.getCon());
		dictionarieBuilder.createTown1Dictionarie(jdbcConnectorMSSQL.getCon());
		dictionarieBuilder.createCountry1Dictionarie(jdbcConnectorMSSQL.getCon());
		dictionarieBuilder.createGroupDictionarie(jdbcConnectorMSSQL.getCon());
		dictionarieBuilder.createSysUserDictionarie(jdbcConnectorMSSQL.getCon());
		
		dictionarieBuilder.buildObjektData(jdbcConnectorMSSQL.getCon(), "DOCUMENT", "DOC", "InsertTimestamp");
		dictionarieBuilder.buildObjektData(jdbcConnectorMSSQL.getCon(), "EMailStore", "EML", "SendDate");
		dictionarieBuilder.buildObjektData(jdbcConnectorMSSQL.getCon(), "APPOINTMENT", "APP", "start_dt");
		dictionarieBuilder.buildObjektData(jdbcConnectorMSSQL.getCon(), "GWOpportunity", "GWOP", "start_dt");
		dictionarieBuilder.buildObjektData(jdbcConnectorMSSQL.getCon(), "gwPhoneCall", "PHC", "InsertTimestamp");
		
		dictionarieBuilder.buildDataWhichGoesOverADay(jdbcConnectorMSSQL.getCon(), "APPOINTMENT", "APP");
		dictionarieBuilder.buildDataWhichGoesOverADay(jdbcConnectorMSSQL.getCon(), "gwPhoneCall", "PHC");
		dictionarieBuilder.buildDataWhichGoesOverADay(jdbcConnectorMSSQL.getCon(), "GWOpportunity", "GWOP");
		
		dictionarieBuilder.buildAppointmentTimeShifts(jdbcConnectorMSSQL.getCon());
		
		System.out.println("EXTRACT ENDED");
		
		return "ETL Prozess";
	}
	
	@GET @Path("/Step2")
	@Produces(MediaType.TEXT_HTML)
	public String requestStep2(@Context UriInfo uriInfo, @Context HttpHeaders headers) {
		
		System.out.println("TRANSFORM STARTED");
		
		transform.transformMultipleAppointment();
		transform.reuniteTables();
		transform.replaceTownAndCountry();
		
		System.out.println("TRANSFORM ENDED");
		
		return "ETL Prozess";
	}
	
	@GET @Path("/Step3")
	@Produces(MediaType.TEXT_HTML)
	public String requestStep3(@Context UriInfo uriInfo, @Context HttpHeaders headers) {
		
		System.out.println("LOAD STARTED");
		
		loader.loadMainData(Database.con);
		loader.loadTownData(Database.con);
		loader.loadCountryData(Database.con);
		loader.loadSysUserData(Database.con);
		loader.loadSysUserGroupData(Database.con);
		
		loader.createIndexPersonID(Database.con);
		
		System.out.println("LOAD ENDED");
		
		return "ETL Prozess";
	}
	
	@GET @Path("/Step4")
	@Produces(MediaType.TEXT_HTML)
	public String requestStep4(@Context UriInfo uriInfo, @Context HttpHeaders headers) {
		
		System.out.println("GET DATA STARTED");
		
//		long zstVorher;
//		long zstNachher;
//
//		zstVorher = System.currentTimeMillis();
		
		String respond = respondQuery.getRespond(Database.con).toString();
		
//		zstNachher = System.currentTimeMillis();
//		String respond = "Zeit ben�tigt: " + (zstNachher - zstVorher) + " ms";
//		
//		System.out.println("GET DATA  ENDED");
		
		return respond;
	}

	@POST @Path("/Step4")
	@Produces(MediaType.TEXT_HTML)
	public String post(JSONObject content, @Context UriInfo uriInfo, @Context HttpHeaders headers) {

		System.out.println(content);
		logik.buildQuery(Database.con, content);
		
		return null;
	}

	@PUT
	@Produces(MediaType.TEXT_HTML)
	public String put(String content, @Context UriInfo uriInfo, @Context HttpHeaders headers) {

		return null;
	}

	@DELETE
	@Produces(MediaType.TEXT_HTML)
	public Response delete(@Context UriInfo uriInfo, @Context HttpHeaders headers) {

		return null;
	}
}