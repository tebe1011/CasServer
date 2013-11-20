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
	
	private String EPATH = System.getProperty("catalina.base") + "/CasAnalyticsData/Extract/";
	private String TPATH = System.getProperty("catalina.base") + "/CasAnalyticsData/Transform/";
	
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
		dictionarieBuilder.build0RelAppointmentDayShifts(jdbcConnectorMSSQL.getCon());
		
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
		
//		transform.addAdressToCSV(4, EPATH + "DOCUMENTORel.csv", EPATH + "DOCUMENTORel_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "EMailStoreORel.csv", EPATH + "EMailStoreORel_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "APPOINTMENTORel.csv", EPATH + "APPOINTMENTORel_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "GWOpportunityORel.csv", EPATH + "GWOpportunityORel_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "gwPhoneCallORel.csv", EPATH + "gwPhoneCallORel_transf.csv");
//		
//		transform.addAdressToCSV(4, EPATH + "Old_APPOINTMENTORel.csv", EPATH + "Old_APPOINTMENTORel_transf.csv");
//		transform.addAdressToCSV(5, EPATH + "Splitted_APPOINTMENTORel.csv", EPATH + "Splitted_APPOINTMENTORel_transf.csv");
//		transform.transformMultipleAppointmentORel();
		
//		transform.transformMultipleAppointment();
		
//		transform.replaceTownAndCountrySysUserWithAdress();
//		transform.replaceTownAndCountrySysUserAndGroupWithAdress();
		
		transform.reuniteTables();
//		transform.replaceTownAndCountry();
//		transform.replaceTownAndCountrySysUserWithAdress();
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
	
	@GET @Path("/Step6")
	@Produces(MediaType.TEXT_HTML)
	public String requestTrans(@Context UriInfo uriInfo, @Context HttpHeaders headers) {

		System.out.println("Step6 STARTED");
		
//		dictionarieBuilder.build0RelData(jdbcConnectorMSSQL.getCon(), "DOCUMENT", "InsertTimestamp", "1");
//		dictionarieBuilder.build0RelData(jdbcConnectorMSSQL.getCon(), "EMailStore", "SendDate", "2");
//		dictionarieBuilder.build0RelData(jdbcConnectorMSSQL.getCon(), "APPOINTMENT", "start_dt", "3");
//		dictionarieBuilder.build0RelData(jdbcConnectorMSSQL.getCon(), "GWOpportunity", "start_dt", "4");
//		dictionarieBuilder.build0RelData(jdbcConnectorMSSQL.getCon(), "gwPhoneCall", "InsertTimestamp", "5");
//		
//		dictionarieBuilder.build0RelAppointmentMultipleDays(jdbcConnectorMSSQL.getCon());
//		dictionarieBuilder.build0RelAppointmentDayShifts(jdbcConnectorMSSQL.getCon());
//		
//		transform.addAdressToCSV(4, EPATH + "DOCUMENT.csv", EPATH + "DOCUMENT_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "EMailStore.csv", EPATH + "EMailStore_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "APPOINTMENT.csv", EPATH + "APPOINTMENT_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "GWOpportunity.csv", EPATH + "GWOpportunity_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "gwPhoneCall.csv", EPATH + "gwPhoneCall_transf.csv");
//		
//		transform.addAdressToCSV(4, EPATH + "DOCUMENTORel.csv", EPATH + "DOCUMENTORel_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "EMailStoreORel.csv", EPATH + "EMailStoreORel_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "APPOINTMENTORel.csv", EPATH + "APPOINTMENTORel_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "GWOpportunityORel.csv", EPATH + "GWOpportunityORel_transf.csv");
//		transform.addAdressToCSV(4, EPATH + "gwPhoneCallORel.csv", EPATH + "gwPhoneCallORel_transf.csv");
//		
//		transform.addAdressToCSV(4, EPATH + "Old_APPOINTMENTORel.csv", EPATH + "Old_APPOINTMENTORel_transf.csv");
//		transform.addAdressToCSV(5, EPATH + "Splitted_APPOINTMENTORel.csv", EPATH + "Splitted_APPOINTMENTORel_transf.csv");
//		transform.transformMultipleAppointmentORel();
		
//		dictionarieBuilder.buildGetGroupChanges(jdbcConnectorMSSQL.getCon());
//		dictionarieBuilder.buildUserGGUID(jdbcConnectorMSSQL.getCon());
//		dictionarieBuilder.buildGroupGGUID(jdbcConnectorMSSQL.getCon());
//		
//		transform.transformGroupHistory();
//		dictionarieBuilder.buildSysUserWithAdress(jdbcConnectorMSSQL.getCon());
//		dictionarieBuilder.buildSysUserAndGroupWithAdress(jdbcConnectorMSSQL.getCon());
//		transform.replaceTownAndCountrySysUserWithAdress();
//		transform.replaceTownAndCountrySysUserAndGroupWithAdress();
//		transform.deleteOrInsertTimeShiftedPeople();
		
//		transform.replaceTownAndCountry(EPATH + "DOCUMENT_transf.csv", EPATH + "DOCUMENT_transf_replaced.csv");
//		transform.replaceTownAndCountry(EPATH + "EMailStore_transf.csv", EPATH + "EMailStore_transf_replaced.csv");
//		transform.replaceTownAndCountry(EPATH + "APPOINTMENT_transf.csv", EPATH + "APPOINTMENT_transf_replaced.csv");
//		transform.replaceTownAndCountry(EPATH + "GWOpportunity_transf.csv", EPATH + "GWOpportunity_transf_replaced.csv");
//		transform.replaceTownAndCountry(EPATH + "gwPhoneCall_transf.csv", EPATH + "gwPhoneCall_transf_replaced.csv");
//		
//		transform.replaceTownAndCountry(EPATH + "Old_APPOINTMENT.csv", EPATH + "Old_APPOINTMENT_replaced.csv");
//		transform.replaceTownAndCountry(TPATH + "S_APPOINTMENT.csv", TPATH + "S_APPOINTMENT_replaced.csv");
		dictionarieBuilder.buildSysGroupRelation(jdbcConnectorMSSQL.getCon());
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
