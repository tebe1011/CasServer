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
		dictionarieBuilder.createClientUser(jdbcConnectorMSSQL.getCon());
		
		dictionarieBuilder.buildObjektData(jdbcConnectorMSSQL.getCon(), "DOCUMENT", "DOC", "InsertTimestamp", "1");
		dictionarieBuilder.buildObjektData(jdbcConnectorMSSQL.getCon(), "EMailStore", "EML", "SendDate", "2");
		dictionarieBuilder.buildObjektData(jdbcConnectorMSSQL.getCon(), "APPOINTMENT", "APP", "start_dt", "3");
		dictionarieBuilder.buildObjektData(jdbcConnectorMSSQL.getCon(), "GWOpportunity", "GWOP", "start_dt", "4");
		dictionarieBuilder.buildObjektData(jdbcConnectorMSSQL.getCon(), "gwPhoneCall", "PHC", "InsertTimestamp", "5");
		
		dictionarieBuilder.buildDataWhichGoesOverADay(jdbcConnectorMSSQL.getCon(), "APPOINTMENT", "APP", "3");
		dictionarieBuilder.buildDataWhichGoesOverADay(jdbcConnectorMSSQL.getCon(), "gwPhoneCall", "PHC", "5");
		dictionarieBuilder.buildDataWhichGoesOverADay(jdbcConnectorMSSQL.getCon(), "GWOpportunity", "GWOP", "4");
		
		dictionarieBuilder.buildAppointmentTimeShifts(jdbcConnectorMSSQL.getCon(), "3");
		
		dictionarieBuilder.buildGetGroupChanges(jdbcConnectorMSSQL.getCon());
		dictionarieBuilder.buildUserGGUID(jdbcConnectorMSSQL.getCon());
		dictionarieBuilder.buildGroupGGUID(jdbcConnectorMSSQL.getCon());
		
		dictionarieBuilder.buildSysGroupRelation(jdbcConnectorMSSQL.getCon());
		
		dictionarieBuilder.buildSysUserWithAdress(jdbcConnectorMSSQL.getCon());
		
		System.out.println("EXTRACT ENDED");
		
		return "ETL Prozess";
	}
	
	@GET @Path("/Step2")
	@Produces(MediaType.TEXT_HTML)
	public String requestStep2(@Context UriInfo uriInfo, @Context HttpHeaders headers) {
		
		System.out.println("TRANSFORM STARTED");
		
//		transform.addAdressDataToORelData("DOCUMENT");
//		transform.addAdressDataToORelData("EMailStore");
//		transform.addAdressDataToORelData("APPOINTMENT");
//		transform.addAdressDataToORelData("GWOpportunity");
//		transform.addAdressDataToORelData("gwPhoneCall");
		
//		transform.addAdressDataToORelApp("Old_APPOINTMENTORel");
//		transform.addAdressDataToORelAppSplitted("Splitted_APPOINTMENTORel");
//		transform.transformMultipleAppointmentORel();
		
		transform.transformMultipleAppointment();
		transform.reuniteTables();
		transform.replaceTownAndCountry();
		transform.replaceTownAndCountrySysUser();
//		transform.transformGroupHistory();
//		transform.deleteOrInsertTimeShiftedPeople();
		
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
		loader.loadClientUserData(Database.con);
		
		loader.createIndexPersonID(Database.con);
		loader.createIndexDateDay(Database.con);
		loader.createIndexOTyp(Database.con);
		
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
//		String respond = "Zeit benötigt: " + (zstNachher - zstVorher) + " ms";
//		
//		System.out.println("GET DATA  ENDED");
		
		return respond;
	}
	
	@GET @Path("/Step6")
	@Produces(MediaType.TEXT_HTML)
	public String requestTrans(@Context UriInfo uriInfo, @Context HttpHeaders headers) {

		System.out.println("Step6 STARTED");
		
//		dictionarieBuilder.build0RelData(jdbcConnectorMSSQL.getCon(), "DOCUMENT", "InsertTimestamp", "1");
//		dictionarieBuilder.build0RelData(jdbcConnectorMSSQL.getCon(), "EMailStore", "SendDate", "2");
//		dictionarieBuilder.build0RelData(jdbcConnectorMSSQL.getCon(), "APPOINTMENT", "start_dt", "3");
//		dictionarieBuilder.build0RelData(jdbcConnectorMSSQL.getCon(), "GWOpportunity", "start_dt", "4");
//		dictionarieBuilder.build0RelData(jdbcConnectorMSSQL.getCon(), "gwPhoneCall", "InsertTimestamp", "5");
		
//		dictionarieBuilder.build0RelAppointmentMultipleDays(jdbcConnectorMSSQL.getCon());
//		dictionarieBuilder.build0RelAppointmentDayShifts(jdbcConnectorMSSQL.getCon());
		
//		transform.addAdressDataToORelData("DOCUMENT");
//		transform.addAdressDataToORelData("EMailStore");
//		transform.addAdressDataToORelData("APPOINTMENT");
//		transform.addAdressDataToORelData("GWOpportunity");
//		transform.addAdressDataToORelData("gwPhoneCall");
//		
//		transform.addAdressDataToORelApp("Old_APPOINTMENTORel");
//		transform.addAdressDataToORelAppSplitted("Splitted_APPOINTMENTORel");
//		transform.transformMultipleAppointmentORel();
		
//		dictionarieBuilder.buildGetGroupChanges(jdbcConnectorMSSQL.getCon());
//		dictionarieBuilder.buildUserGGUID(jdbcConnectorMSSQL.getCon());
//		dictionarieBuilder.buildGroupGGUID(jdbcConnectorMSSQL.getCon());
//		
//		transform.transformGroupHistory();
		
//		transform.deleteOrInsertTimeShiftedPeople();
		
		System.out.println("Step6 ENDED");
		
		return "";
	}

	@POST @Path("/Step4")
	@Produces(MediaType.APPLICATION_JSON)
	public JSONObject post(JSONObject content, @Context UriInfo uriInfo, @Context HttpHeaders headers) {

		JSONObject json = logik.buildQuery(Database.con, content);
		
		return json;
	}

	@POST @Path("/UserData")
	@Produces(MediaType.TEXT_HTML)
	public String requestUserData(String content, @Context UriInfo uriInfo, @Context HttpHeaders headers) {
		
		String result = "";
		
		System.out.println("GET USER DATA STARTED");
		
		result = dictionarieBuilder.checkUserInDatabase(Database.con, content);
		
		System.out.println("GET USER DATA  ENDED");
		
		return result;
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
