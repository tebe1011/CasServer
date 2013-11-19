package de.cas.etl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

public class Transform {

	private CSVBuilder csvBuilder = new CSVBuilder();
	private CSVReader reader;
	private String extractPath = System.getProperty("catalina.base") + "/CasAnalyticsData/Extract/";
	private String transformPath = System.getProperty("catalina.base") + "/CasAnalyticsData/Transform/";

	public Transform() {

	}

	public void transformMultipleAppointment() {
		CSVReader reader;
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			reader = new CSVReader(new FileReader(extractPath + "Splitted_APPOINTMENT.csv"));
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
			reader = new CSVReader(new FileReader(extractPath + "Splitted_APPOINTMENTORel_transf.csv"));
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
		result.addAll(getDataFromCSVFile(extractPath + "APPOINTMENT.csv"));
		result.addAll(getDataFromCSVFile(extractPath + "DOCUMENT.csv"));
		result.addAll(getDataFromCSVFile(extractPath + "gwPhoneCall.csv"));
		result.addAll(getDataFromCSVFile(extractPath + "GWOpportunity.csv"));
		result.addAll(getDataFromCSVFile(extractPath + "EMailStore.csv"));
		result.addAll(getDataFromCSVFile(transformPath + "S_APPOINTMENT.csv"));
		result.addAll(getDataFromCSVFile(extractPath + "Old_APPOINTMENT.csv"));
		result.addAll(getDataFromCSVFile(extractPath + "APPOINTMENTORel_transf.csv"));
		result.addAll(getDataFromCSVFile(extractPath + "DOCUMENTORel_transf.csv"));
		result.addAll(getDataFromCSVFile(extractPath + "EMailStoreORel_transf.csv"));
		result.addAll(getDataFromCSVFile(extractPath + "GWOpportunityORel_transf.csv"));
		result.addAll(getDataFromCSVFile(extractPath + "gwPhoneCallORel_transf.csv"));
		result.addAll(getDataFromCSVFile(extractPath + "Old_APPOINTMENTORel_transf.csv"));
		result.addAll(getDataFromCSVFile(transformPath + "S_APPOINTMENTORel.csv"));
		
		Collections.sort(result, new Comparator<String[]>() {
			@Override
			public int compare(String[] arg0, String[] arg1) {
				int cmp = Integer.parseInt(arg0[0]) - Integer.parseInt(arg1[0]);
				return cmp;
			}
		});
		
		for(int i = 0; i < result.size(); ++i) {
			if(result.get(i)[4].equals("null") || result.get(i)[4].equals("")) {
				result.get(i)[4] = "1";
			}
			if(result.get(i)[5].equals("null") || result.get(i)[5].equals("")) {
				result.get(i)[5] = "1";
			}
			if(result.get(i)[6].equals("null") || result.get(i)[6].equals("")) {
				result.get(i)[6] = "1";
			}
			if(result.get(i)[7].equals("null") || result.get(i)[7].equals("")) {
				result.get(i)[7] = "0";
			}
			if(result.get(i)[8].equals("null") || result.get(i)[8].equals("")) {
				result.get(i)[8] = "0";
			}
			if(result.get(i)[1].equals("")) {
				result.remove(i);
				i -= 1;
			}
		}
		
		try {
			File file = new File(transformPath + "reunited_sorted.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < result.size(); ++i) {
				writer.writeNext(result.get(i));
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		result = null;
	}

	private ArrayList<String[]> getDataFromCSVFile(String filePath) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			reader = new CSVReader(new FileReader(filePath));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				result.add(new String[] {nextLine[0], nextLine[1], nextLine[2], nextLine[3], nextLine[4],
						nextLine[5], nextLine[6], nextLine[7], nextLine[8]});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public void replaceTownAndCountry() {
		
		ArrayList<String[]> mainData = new ArrayList<String[]>();
		try {
			reader = new CSVReader(new FileReader(transformPath + "reunited_sorted.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				mainData.add(new String[] {nextLine[0], nextLine[1], nextLine[2], nextLine[3], nextLine[4],
						nextLine[5], nextLine[6], nextLine[7], nextLine[8]});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Map<String,Integer> townDictionary = new HashMap<String,Integer>();
		Map<String,Integer> countryDictionary = new HashMap<String,Integer>();
		
		try {
			reader = new CSVReader(new FileReader(extractPath + "Town.csv"));
			String[] nextLineT;
			while ((nextLineT = reader.readNext()) != null) {
				townDictionary.put(nextLineT[1], Integer.parseInt(nextLineT[0]));
			}
			reader = new CSVReader(new FileReader(extractPath + "Country.csv"));
			String[] nextLineC;
			while ((nextLineC = reader.readNext()) != null) {
				countryDictionary.put(nextLineC[1], Integer.parseInt(nextLineC[0]));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
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
				if(mainData.get(i)[1].equals("")) {
					mainData.remove(i);
					--i;
				}
			}
		}
		
		Collections.sort(mainData, new Comparator<String[]>() {
			@Override
			public int compare(String[] arg0, String[] arg1) {
				int cmp = Integer.parseInt(arg0[0]) - Integer.parseInt(arg1[0]);
				if(cmp == 0) {
					cmp  = Integer.parseInt(arg0[1]) - Integer.parseInt(arg1[1]);
				}
				return cmp;
			}
		});
		
		try {
			File file = new File(transformPath + "reunited_sorted_replaced.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < mainData.size(); ++i) {
				writer.writeNext(mainData.get(i));
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		mainData = null;
	}
	
	public void transformGroupHistory() {
		Map<String, String> gguidGroup = new HashMap<String, String>();
		try {
			reader = new CSVReader(new FileReader(extractPath + "GGUID_Group.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				gguidGroup.put(nextLine[0], nextLine[1]);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Map<String, String> gguidUser = new HashMap<String, String>();
		try {
			reader = new CSVReader(new FileReader(extractPath + "GGUID_User.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				gguidUser.put(nextLine[0], nextLine[1]);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		ArrayList<String[]> mainData = new ArrayList<String[]>();
		try {
			reader = new CSVReader(new FileReader(extractPath + "GroupHistory.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				mainData.add(new String[] {nextLine[0], nextLine[1], nextLine[2], nextLine[3]});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String[] s : mainData) {
			
			if(s[1].equals("A")) {
				s[1] = "1";
			}
			else {
				s[1] = "0";
			}
			
			s[2] = gguidUser.get(s[2]);
			s[3] = gguidGroup.get(s[3]);
		}
		
		try {
			File file = new File(transformPath + "GroupHistory.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < mainData.size(); ++i) {
				writer.writeNext(mainData.get(i));
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void replaceTownAndCountrySysUser() {
		ArrayList<String[]> mainData = new ArrayList<String[]>();
		try {
			reader = new CSVReader(new FileReader(extractPath + "SysUserWIthAdress.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				mainData.add(new String[] {nextLine[0], nextLine[1], nextLine[2], nextLine[3], nextLine[4], nextLine[5]});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		Map<String,Integer> townDictionary = new HashMap<String,Integer>();
		Map<String,Integer> countryDictionary = new HashMap<String,Integer>();
		
		try {
			reader = new CSVReader(new FileReader(extractPath + "Town.csv"));
			String[] nextLineT;
			while ((nextLineT = reader.readNext()) != null) {
				townDictionary.put(nextLineT[1], Integer.parseInt(nextLineT[0]));
			}
			reader = new CSVReader(new FileReader(extractPath + "Country.csv"));
			String[] nextLineC;
			while ((nextLineC = reader.readNext()) != null) {
				countryDictionary.put(nextLineC[1], Integer.parseInt(nextLineC[0]));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(int i = 0; i < mainData.size(); ++i) {
			mainData.get(i)[4] = String.valueOf(townDictionary.get(mainData.get(i)[4]));
			mainData.get(i)[5] = String.valueOf(countryDictionary.get(mainData.get(i)[5]));
			if(mainData.get(i)[4].equals("null")) {
				mainData.get(i)[4] = String.valueOf(townDictionary.get("null"));
			}
			if(mainData.get(i)[1].equals("")) {
				mainData.remove(i);
				--i;
			}
		}
		
		try {
			File file = new File(transformPath + "SysUserWIthAdress.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < mainData.size(); ++i) {
				writer.writeNext(mainData.get(i));
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteOrInsertTimeShiftedPeople() {
		
//		Map<String, Integer[]> adress = new HashMap<String, Integer[]>();
//		try {
//			reader = new CSVReader(new FileReader(transformPath+"SysUserWIthAdress.csv"));
//			String[] nextLine;
//			while ((nextLine = reader.readNext()) != null) {
//				adress.put(nextLine[0], new Integer[] {Integer.parseInt(nextLine[0]), Integer.parseInt(nextLine[1]),
//						Integer.parseInt(nextLine[2]), Integer.parseInt(nextLine[3]), Integer.parseInt(nextLine[4]), Integer.parseInt(nextLine[5])});
//			}
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//		ArrayList<Integer[]> grouHistory = new ArrayList<Integer[]>();
//		Map<String, Integer> groupGID = new HashMap<String, Integer>();
//		try {
//			reader = new CSVReader(new FileReader(transformPath+"GroupHistory.csv"));
//			String[] nextLine;
//			int index = 0;
//			while ((nextLine = reader.readNext()) != null) {
//				grouHistory.add(new Integer[] {Integer.parseInt(nextLine[0]), Integer.parseInt(nextLine[1]), Integer.parseInt(nextLine[2]), Integer.parseInt(nextLine[3])});
//				groupGID.put(nextLine[3], index);
//				index += 1;
//			}
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
		ArrayList<Integer[]> mainData = new ArrayList<Integer[]>();
		try {
			reader = new CSVReader(new FileReader(transformPath + "reunited_sorted_replaced.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				mainData.add(new Integer[] {Integer.parseInt(nextLine[0]), Integer.parseInt(nextLine[1]), Integer.parseInt(nextLine[2]),
						Integer.parseInt(nextLine[3]), Integer.parseInt(nextLine[4]), Integer.parseInt(nextLine[5]), Integer.parseInt(nextLine[6]), 
						Integer.parseInt(nextLine[7]), Integer.parseInt(nextLine[8])});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		for(int i = 0; i < mainData.size(); ++i) {
//			if(mainData.get(i)[10].equals("1")) {
//				if(groupGID.get(mainData.get(i)[9]) != null) {
//					int curentDate = mainData.get(i)[1];
//					for(Integer [] s : grouHistory) {
//						if(s[0] > curentDate) {
//							if(s[1] == 1) {
//								boolean end = false;
//								int c = 0;
//								int gid = s[3];
//								while(end == false) {
//									if(s[2] == mainData.get(i+c)[3]) {
//										mainData.remove(i+c);
//										c -= 1;
//									}
//									if(gid != mainData.get(i+c)[9]) {
//										break;
//									}
//									c += 1;
//								}
//							}
//							else if (s[1] == 0) {
//								boolean end = false;
//								boolean found = false;
//								int c = 0;
//								int gid = s[3];
//								while(end == false) {
//									if(s[2] == mainData.get(i+c)[3]) {
//										found = true;
//										c -= 1;
//									}
//									if(gid != mainData.get(i+c)[9]) {
//										break;
//									}
//									c += 1;
//								}
//								if(found == false) {
//									Integer[] data = adress.get(mainData.get(i)[1]);
//									mainData.add(new Integer[] {data[0], mainData.get(i)[2], mainData.get(i)[3], s[2],
//											data[1], data[2], data[3], data[4], data[5], s[3]});
//								}
//							}
//						}
//					}
//				}
//			}
//		}
//		System.out.println("done.");
		try {
			File file = new File(transformPath + "reunited_sorted_replaced2.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < mainData.size(); ++i) {
				
				writer.writeNext(new String[] {""+mainData.get(i)[0], ""+mainData.get(i)[1], ""+mainData.get(i)[2], ""+mainData.get(i)[3], ""+mainData.get(i)[4],
						""+mainData.get(i)[5], ""+mainData.get(i)[6], ""+mainData.get(i)[7], ""+mainData.get(i)[8]});
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void addAdressDataToORelData(String table) {
		
		Map<String, Integer[]> sysUserWithAdress = new HashMap<String, Integer[]>();
		try {
			reader = new CSVReader(new FileReader(transformPath + "SysUserWIthAdress.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				sysUserWithAdress.put(nextLine[0], new Integer[] {Integer.parseInt(nextLine[0]),
						Integer.parseInt(nextLine[1]), Integer.parseInt(nextLine[2]), Integer.parseInt(nextLine[3]),
								Integer.parseInt(nextLine[4]), Integer.parseInt(nextLine[5])});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		ArrayList<String[]> mainData = new ArrayList<String[]>();
		try {
			reader = new CSVReader(new FileReader(extractPath + "" + table + "ORel.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				mainData.add(new String[] {nextLine[0], nextLine[1], nextLine[2], nextLine[3], nextLine[4],
						nextLine[5], nextLine[6], nextLine[7], nextLine[8]});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String[] data : mainData) {
			if(Integer.parseInt(data[3]) <= 0 || sysUserWithAdress.get(data[3]) ==  null) {
				data[4] = "0";
				data[5] = "0";
				data[6] = "0";
				data[7] = "0";
				data[8] = "0";
			}
			else {
				Integer [] dic = sysUserWithAdress.get(data[3]);
					data[4] = ""+dic[1];
					data[5] = ""+dic[2];
					data[6] = ""+dic[3];
					data[7] = ""+dic[4];
					data[8] = ""+dic[5];	
			}
		}
		
		try {
			File file = new File(extractPath + "" + table + "ORel_transf.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < mainData.size(); ++i) {
				
				writer.writeNext(new String[] {mainData.get(i)[0], mainData.get(i)[1], mainData.get(i)[2], mainData.get(i)[3], mainData.get(i)[4],
						mainData.get(i)[5], mainData.get(i)[6], mainData.get(i)[7], mainData.get(i)[8]});
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void addAdressDataToORelApp(String table) {
		
		Map<String, Integer[]> sysUserWithAdress = new HashMap<String, Integer[]>();
		try {
			reader = new CSVReader(new FileReader(transformPath + "SysUserWIthAdress.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				sysUserWithAdress.put(nextLine[0], new Integer[] {Integer.parseInt(nextLine[0]),
						Integer.parseInt(nextLine[1]), Integer.parseInt(nextLine[2]), Integer.parseInt(nextLine[3]),
								Integer.parseInt(nextLine[4]), Integer.parseInt(nextLine[5])});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		ArrayList<String[]> mainData = new ArrayList<String[]>();
		try {
			reader = new CSVReader(new FileReader(extractPath + "" + table + ".csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				mainData.add(new String[] {nextLine[0], nextLine[1], nextLine[2], nextLine[3], nextLine[4],
						nextLine[5], nextLine[6], nextLine[7], nextLine[8]});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String[] data : mainData) {
			if(Integer.parseInt(data[3]) <= 0 || sysUserWithAdress.get(data[3]) ==  null) {
				data[4] = "0";
				data[5] = "0";
				data[6] = "0";
				data[7] = "0";
				data[8] = "0";
			}
			else {
				Integer [] dic = sysUserWithAdress.get(""+data[3]);
				data[4] = ""+dic[1];
				data[5] = ""+dic[2];
				data[6] = ""+dic[3];
				data[7] = ""+dic[4];
				data[8] = ""+dic[5];
			}
		}
		
		try {
			File file = new File(extractPath + "" + table + "_transf.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < mainData.size(); ++i) {
				
				writer.writeNext(new String[] {""+mainData.get(i)[0], ""+mainData.get(i)[1], ""+mainData.get(i)[2], ""+mainData.get(i)[3], ""+mainData.get(i)[4],
						""+mainData.get(i)[5], ""+mainData.get(i)[6], ""+mainData.get(i)[7], ""+mainData.get(i)[8]});
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void addAdressDataToORelAppSplitted(String table) {
		
		Map<String, Integer[]> sysUserWithAdress = new HashMap<String, Integer[]>();
		try {
			reader = new CSVReader(new FileReader(transformPath + "SysUserWIthAdress.csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				sysUserWithAdress.put(nextLine[0], new Integer[] {Integer.parseInt(nextLine[0]),
						Integer.parseInt(nextLine[1]), Integer.parseInt(nextLine[2]), Integer.parseInt(nextLine[3]),
								Integer.parseInt(nextLine[4]), Integer.parseInt(nextLine[5])});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		ArrayList<String[]> mainData = new ArrayList<String[]>();
		try {
			reader = new CSVReader(new FileReader(extractPath + "" + table + ".csv"));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				mainData.add(new String[] {nextLine[0], nextLine[1], nextLine[2], nextLine[3], nextLine[4],
						nextLine[5], nextLine[6], nextLine[7], nextLine[8], nextLine[9]});
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		for(String[] data : mainData) {
			if(Integer.parseInt(data[3]) <= 0 || sysUserWithAdress.get(data[3]) ==  null) {
				data[5] = "0";
				data[6] = "0";
				data[7] = "0";
				data[8] = "0";
				data[9] = "0";
			}
			else {
				Integer [] dic = sysUserWithAdress.get(""+data[3]);
				data[5] = ""+dic[1];
				data[6] = ""+dic[2];
				data[7] = ""+dic[3];
				data[8] = ""+dic[4];
				data[9] = ""+dic[5];
			}
		}
		
		try {
			File file = new File(extractPath + "" + table + "_transf.csv");
			CSVWriter writer = new CSVWriter(new FileWriter(file));
			for (int i = 0; i < mainData.size(); ++i) {
				
				writer.writeNext(new String[] {mainData.get(i)[0], mainData.get(i)[1], mainData.get(i)[2], mainData.get(i)[3], mainData.get(i)[4],
						mainData.get(i)[5], mainData.get(i)[6], mainData.get(i)[7], mainData.get(i)[8], mainData.get(i)[9]});
			}
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
