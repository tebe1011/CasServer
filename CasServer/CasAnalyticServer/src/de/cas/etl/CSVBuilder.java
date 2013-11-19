package de.cas.etl;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import au.com.bytecode.opencsv.CSVWriter;

public class CSVBuilder {

	public CSVBuilder() {

	}

	public void buildDictionarie(String fileName, ArrayList<String> data) {
		try {
			String basePath = System.getProperty("catalina.base") + "/CasAnalyticsData/Extract";
			File dictionaries = new File(basePath);
			dictionaries.mkdir();
			File file = new File(basePath + "/" + fileName + ".csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < data.size(); ++i) {
				if (data.get(i) != null) {
					writer.writeNext(new String[] { "" + i, data.get(i).replace("\n", "") });
				} else {
					writer.writeNext(new String[] { "" + i, "null" });
				}
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void buildDictionarieWithOID(String fileName, ArrayList<String[]> data) {
		try {
			String basePath = System.getProperty("catalina.base") + "/CasAnalyticsData/Extract";
			File dictionaries = new File(basePath);
			dictionaries.mkdir();
			File file = new File(basePath + "/" + fileName + ".csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < data.size(); ++i) {
				if (data.get(i) != null) {
					writer.writeNext(new String[] {data.get(i)[1], data.get(i)[0].replace("\n", "") });
				} else {
					writer.writeNext(new String[] { "" + i, "null" });
				}
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void buildTableRelation(String fileName, ArrayList<String[]> data) {
		try {
			String basePath = System.getProperty("catalina.base") + "/CasAnalyticsData/Extract";
			File dictionaries = new File(basePath);
			dictionaries.mkdir();
			File file = new File(basePath + "/" + fileName + ".csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < data.size(); ++i) {
				writer.writeNext(data.get(i));
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void buildTransformMultipleAppointment(ArrayList<String[]> data) {
		try {
			String basePath = System.getProperty("catalina.base") + "/CasAnalyticsData/Transform";
			File dictionaries = new File(basePath);
			dictionaries.mkdir();
			File file = new File(basePath + "/S_APPOINTMENT.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < data.size(); ++i) {
				writer.writeNext(data.get(i));
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void buildTransformMultipleAppointmentORel(ArrayList<String[]> data) {
		try {
			String basePath = System.getProperty("catalina.base") + "/CasAnalyticsData/Transform";
			File dictionaries = new File(basePath);
			dictionaries.mkdir();
			File file = new File(basePath + "/S_APPOINTMENTORel.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < data.size(); ++i) {
				writer.writeNext(data.get(i));
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void buildGroupHistory(ArrayList<String[]> data) {
		try {
			String basePath = System.getProperty("catalina.base") + "/CasAnalyticsData/Extract";
			File dictionaries = new File(basePath);
			dictionaries.mkdir();
			File file = new File(basePath + "/GroupHistory.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < data.size(); ++i) {
				writer.writeNext(data.get(i));
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void buildGGUIDDictionarie(String fileName, ArrayList<String[]> data) {
		try {
			String basePath = System.getProperty("catalina.base") + "/CasAnalyticsData/Extract";
			File dictionaries = new File(basePath);
			dictionaries.mkdir();
			File file = new File(basePath + "/" + fileName + ".csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < data.size(); ++i) {
				writer.writeNext(data.get(i));
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void buildSysUserWithAdress(String fileName, ArrayList<String[]> data) {
		try {
			String basePath = System.getProperty("catalina.base") + "/CasAnalyticsData/Extract";
			File dictionaries = new File(basePath);
			dictionaries.mkdir();
			File file = new File(basePath + "/" + fileName + ".csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < data.size(); ++i) {
				writer.writeNext(data.get(i));
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
