package de.cas.etl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
				for (int i = 1; i < Integer.parseInt(nextLine[10]); ++i) {
					String day = String.valueOf(Integer.parseInt(nextLine[1]) + i);
					result.add(new String[] { nextLine[0], day, nextLine[2], nextLine[3],
							nextLine[4], nextLine[5], nextLine[6], nextLine[7], nextLine[8], nextLine[9]});
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		csvBuilder.buildTransformMultipleAppointment(result);
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
		
		Collections.sort(result, new Comparator<String[]>() {
			@Override
			public int compare(String[] arg0, String[] arg1) {
				return Integer.parseInt(arg0[0]) - Integer.parseInt(arg1[0]);
			}
		});
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
	}

	private ArrayList<String[]> getDataFromCSVFile(String filePath) {
		ArrayList<String[]> result = new ArrayList<String[]>();
		try {
			reader = new CSVReader(new FileReader(filePath));
			String[] nextLine;
			while ((nextLine = reader.readNext()) != null) {
				result.add(new String[] {nextLine[0], nextLine[1], nextLine[2], nextLine[3], nextLine[4],
						nextLine[5], nextLine[6], nextLine[7], nextLine[8], nextLine[9]});
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
			reader = new CSVReader(new FileReader(transformPath+"reunited_sorted.csv"));
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
		
		Map<String,Integer> townDictionary = new HashMap<String,Integer>();
		Map<String,Integer> countryDictionary = new HashMap<String,Integer>();
		
		try {
			reader = new CSVReader(new FileReader(extractPath+"Town.csv"));
			String[] nextLineT;
			while ((nextLineT = reader.readNext()) != null) {
				townDictionary.put(nextLineT[1], Integer.parseInt(nextLineT[0]));
			}
			reader = new CSVReader(new FileReader(extractPath+"Country.csv"));
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
			mainData.get(i)[7] = String.valueOf(townDictionary.get(mainData.get(i)[7]));
			mainData.get(i)[8] = String.valueOf(countryDictionary.get(mainData.get(i)[8]));
			if(mainData.get(i)[7].equals("null")) {
				mainData.get(i)[7] = String.valueOf(townDictionary.get("null"));
			}
			if(mainData.get(i)[1].equals("")) {
				mainData.remove(i);
				--i;
			}
		}
		
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
	}
}
