package de.cas.etl;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import au.com.bytecode.opencsv.CSVReader;

public class Transform {

	private CSVBuilder csvBuilder = new CSVBuilder();
	private CSVReaderWriter csv= new CSVReaderWriter(); 
	
	private String EPATH = System.getProperty("catalina.base") + "/CasAnalyticsData/Extract/";
	private String TPATH = System.getProperty("catalina.base") + "/CasAnalyticsData/Transform/";

	public Transform() {

	}

	public void transformMultipleAppointment() {
		CSVReader reader;
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			reader = new CSVReader(new FileReader(EPATH + "Splitted_APPOINTMENT.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				for (int i = 1; i < Integer.parseInt(nextLine[9]); ++i) {
					String day = String.valueOf(Integer.parseInt(nextLine[1]) + i);
					result.add(new String[] { nextLine[0], day, nextLine[2], nextLine[3],
							nextLine[4], nextLine[5], nextLine[6], nextLine[7], nextLine[8]});
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		csvBuilder.buildTransformMultipleAppointment(result);
	}
	
	public void transformMultipleAppointmentORel() {
		CSVReader reader;
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			reader = new CSVReader(new FileReader(EPATH + "Splitted_APPOINTMENTORel_transf.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				for (int i = 1; i < Integer.parseInt(nextLine[9]); ++i) {
					String day = String.valueOf(Integer.parseInt(nextLine[1]) + i);
					result.add(new String[] { nextLine[0], day, nextLine[2], nextLine[3],
							nextLine[4], nextLine[5], nextLine[6], nextLine[7], nextLine[8]});
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		csvBuilder.buildTransformMultipleAppointmentORel(result);
	}

	public void reuniteTables() {

		ArrayList<String[]> result = new ArrayList<String[]>();
		
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "APPOINTMENT_transf_replaced.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "DOCUMENT_transf_replaced.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "gwPhoneCall_transf_replaced.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "GWOpportunity_transf_replaced.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "EMailStore_transf_replaced.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "Old_APPOINTMENTORel_transf.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "Old_APPOINTMENT_transf_replaced.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "APPOINTMENTORel_transf.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "DOCUMENTORel_transf.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "EMailStoreORel_transf.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "GWOpportunityORel_transf.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "gwPhoneCallORel_transf.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(TPATH + "S_APPOINTMENTORel.csv"));
		result.trimToSize();
		result.addAll(csv.readFromCSVFileAsStringArray(EPATH + "S_APPOINTMENT_transf_replaced.csv"));
		result.trimToSize();
		
		System.out.println("geklappt");
		
		Collections.sort(result, new Comparator<String[]>() {
			@Override
			public int compare(String[] arg0, String[] arg1) {
				int cmp = Integer.parseInt(arg0[0]) - Integer.parseInt(arg1[0]);
				if(cmp == 0) {
					cmp  = Integer.parseInt(arg0[1]) - Integer.parseInt(arg1[1]);
				}
				return cmp;
			}
		});
		
		System.out.println("geklappt2");
		
		csv.writeDataToCSV(result, TPATH + "reunited_sorted_replaced.csv");
		result = null;
	}
	
	public void replaceTownAndCountry(String inputPath, String outputPath) {
		
		ArrayList<String[]> mainData = csv.readFromCSVFileAsStringArray(inputPath);
		Map<String,Integer> townDictionary = csv.readFromCSVFileAsStringIntegerMap(EPATH + "Town.csv");
		Map<String,Integer> countryDictionary = csv.readFromCSVFileAsStringIntegerMap(EPATH + "Country.csv");
		
		for(int i = 0; i < mainData.size(); ++i) {
			if(mainData.get(i)[7].equals("0") && mainData.get(i)[8].equals("0")) {
			}
			else {
				mainData.get(i)[7] = String.valueOf(townDictionary.get(mainData.get(i)[7]));
				mainData.get(i)[8] = String.valueOf(countryDictionary.get(mainData.get(i)[8]));
				if(mainData.get(i)[7].equals("null")) {
					mainData.get(i)[7] = String.valueOf(townDictionary.get("null"));
				}
				if(mainData.get(i)[8].equals("null")) {
					mainData.get(i)[8] = String.valueOf(townDictionary.get("null"));
				}
				if(mainData.get(i)[0].equals("") || mainData.get(i)[1].equals("") || mainData.get(i)[2].equals("") || mainData.get(i)[3].equals("") ||
						mainData.get(i)[0].equals("null") || mainData.get(i)[1].equals("null") || mainData.get(i)[2].equals("null") || mainData.get(i)[3].equals("null")){
					mainData.remove(i);
					i -=1;
				}
			}
		}
		csv.writeDataToCSV(mainData, outputPath);
		mainData = null;
	}
	
	public void transformGroupHistory() {
		
		Map<String, String> gguidGroup = csv.readFromCSVFileAsStringStringMap(EPATH + "GGUID_Group.csv");
		Map<String, String> gguidUser = csv.readFromCSVFileAsStringStringMap(EPATH + "GGUID_User.csv");
		ArrayList<String[]> data = csv.readFromCSVFileAsStringArray(EPATH + "GroupHistory.csv");
		
		for(String[] s : data) {
			if(s[1].equals("A")) {
				s[1] = "1";
			}
			else {
				s[1] = "0";
			}
			s[2] = gguidUser.get(s[2]);
			s[3] = gguidGroup.get(s[3]);
		}
		csv.writeDataToCSV(data, TPATH + "GroupHistory.csv");
	}
	
	public void replaceTownAndCountrySysUserAndGroupWithAdress() {
		
		ArrayList<String[]> mainData = csv.readFromCSVFileAsStringArray(EPATH + "SysUserAndGroupWithAdress.csv");
		Map<String,Integer> townDictionary = csv.readFromCSVFileAsStringIntegerMap(EPATH + "Town.csv");
		Map<String,Integer> countryDictionary = csv.readFromCSVFileAsStringIntegerMap(EPATH + "Country.csv");

		for(int i = 0; i < mainData.size(); ++i) {
			if(!mainData.get(i)[5].equals("0")) {
				mainData.get(i)[5] = String.valueOf(townDictionary.get(mainData.get(i)[5]));
			}
			if(!mainData.get(i)[6].equals("0")) {
				mainData.get(i)[6] = String.valueOf(countryDictionary.get(mainData.get(i)[6]));
			}
			if(mainData.get(i)[5].equals("null")){
				mainData.get(i)[5] = "0";
			}
		}
		csv.writeDataToCSV(mainData, TPATH + "SysUserAndGroupWithAdress.csv");
	}
	
	public void replaceTownAndCountrySysUserWithAdress() {
		
		ArrayList<String[]> mainData = csv.readFromCSVFileAsStringArray(EPATH + "SysUserWithAdress.csv");
		Map<String,Integer> townDictionary = csv.readFromCSVFileAsStringIntegerMap(EPATH + "Town.csv");
		Map<String,Integer> countryDictionary = csv.readFromCSVFileAsStringIntegerMap(EPATH + "Country.csv");

		for(int i = 0; i < mainData.size(); ++i) {
			if(!mainData.get(i)[4].equals("0")) {
				mainData.get(i)[4] = String.valueOf(townDictionary.get(mainData.get(i)[4]));
			}
			if(!mainData.get(i)[5].equals("0")) {
				mainData.get(i)[5] = String.valueOf(countryDictionary.get(mainData.get(i)[5]));
			}
			if(mainData.get(i)[4].equals("null")){
				mainData.get(i)[4] = "0";
			}
		}
		csv.writeDataToCSV(mainData, TPATH + "SysUserWithAdress.csv");
	}
	
	public void addAdressToCSV(Integer index ,String inputPath, String outputPath) {
		
		Map<String, Integer[]> sysUserWithAdress = csv.readFromCSVFileAsStringIntegerArrayMap(0, TPATH + "SysUserWithAdress.csv");
		ArrayList<String[]> mainData = csv.readFromCSVFileAsStringArray(inputPath);
		
		for(int i = 0; i < mainData.size(); ++i) {
			if(mainData.get(i)[3] == null || mainData.get(i)[0].equals("") || mainData.get(i)[1].equals("") || mainData.get(i)[2].equals("") || mainData.get(i)[3].equals("")){
				mainData.remove(i);
				i -=1;
			}
			else if(Integer.parseInt(mainData.get(i)[0]) == Integer.parseInt(mainData.get(i)[3])){
				mainData.remove(i);
				i -=1;
			}
			else if(sysUserWithAdress.get(mainData.get(i)[3]) == null) {
				mainData.get(i)[index] = "0";
				mainData.get(i)[index + 1] = "0";
				mainData.get(i)[index + 2] = "1";
				mainData.get(i)[index + 3] = "0";
				mainData.get(i)[index + 4] = "0";
			}
			else if(mainData.get(i)[4].equals("") || mainData.get(i)[4].equals("null")) {
				mainData.get(i)[index] = "0";
				mainData.get(i)[index + 1] = "0";
				mainData.get(i)[index + 2] = "1";
				mainData.get(i)[index + 3] = "0";
				mainData.get(i)[index + 4] = "0";
			}
			else {
				Integer [] dic = sysUserWithAdress.get(mainData.get(i)[3]);
					mainData.get(i)[index] = "" + dic[1];
					mainData.get(i)[index + 1] = "" + dic[2];
					mainData.get(i)[index + 2] = "" + dic[3];
					mainData.get(i)[index + 3] = "" + dic[4];
					mainData.get(i)[index + 4] = "" + dic[5];
			}
		}
		csv.writeDataToCSV(mainData, outputPath);
	}
}
