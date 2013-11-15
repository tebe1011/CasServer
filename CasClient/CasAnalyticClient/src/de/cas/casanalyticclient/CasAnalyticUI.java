package de.cas.casanalyticclient;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.annotation.WebServlet;

import au.com.bytecode.opencsv.CSVReader;

import com.vaadin.annotations.Theme;
import com.vaadin.annotations.VaadinServletConfiguration;
import com.vaadin.server.VaadinRequest;
import com.vaadin.server.VaadinServlet;
import com.vaadin.ui.Button;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.CssLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.TextArea;
import com.vaadin.ui.TextField;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;

@Theme("casanalyticclient")
public class CasAnalyticUI extends UI {

	private TextArea textArea;
	private TextField textField;
	private RootUI root;
	
	private String extractPath = System.getProperty("catalina.base") + "/CasAnalyticsData/Extract/";
	private String transformPath = System.getProperty("catalina.base") + "/CasAnalyticsData/Transform/";
	
	static Map<String,String> sysUser = new HashMap<String,String>();
	static Map<String,String> sysGroup = new HashMap<String,String>();
	
//	private JerseyClient restClient = new JerseyClient();

	@WebServlet(value = "/*", asyncSupported = true)
	@VaadinServletConfiguration(productionMode = false, ui = CasAnalyticUI.class)
	public static class Servlet extends VaadinServlet implements Serializable {
	}

	@Override
	protected void init(VaadinRequest request) {

		if(sysUser.isEmpty()) {
			CSVReader reader;
			try {
				reader = new CSVReader(new FileReader(extractPath + "ClientUser.csv"));
				String[] nextLine;
				while ((nextLine = reader.readNext()) != null) {
					sysUser.put(nextLine[0], nextLine[1]);
				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if(sysGroup.isEmpty()) {
			CSVReader reader;
			try {
				reader = new CSVReader(new FileReader(extractPath + "SysGroup.csv"));
				String[] nextLine;
				while ((nextLine = reader.readNext()) != null) {
					sysGroup.put(nextLine[0], nextLine[1]);
				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		root = new RootUI(this);
		setContent(root);
		
//		CssLayout root = new CssLayout();
//		setContent(root);
//		
//		Button button = new Button("Extract Data");
//		button.addClickListener(new Button.ClickListener() {
//			public void buttonClick(ClickEvent event) {
//				restClient.doGetRequestStep1();
//			}
//		});
//
//		Button button2 = new Button("Transform Data");
//		button2.addClickListener(new Button.ClickListener() {
//			public void buttonClick(ClickEvent event) {
//				restClient.doGetRequestStep2();
//			}
//		});
//
//		Button button3 = new Button("Load Data");
//		button3.addClickListener(new Button.ClickListener() {
//			public void buttonClick(ClickEvent event) {
//				restClient.doGetRequestStep3();
//			}
//		});

//		Button button4 = new Button("Get Data");
//		button4.addClickListener(new Button.ClickListener() {
//			public void buttonClick(ClickEvent event) {
//
//				ArrayList<String[]> respond = restClient.doGetRequestStep4(textField.getValue());
//				String value = "";
//				for (String[] s : respond) {
//					value += s[0] + ":" + value + s[1] + " ";
//				}
//
//				textArea.setValue(restClient.doGetRequestStep4(textField.getValue()));
//			}
//		});

//		textField = new TextField();
//
//		textArea = new TextArea();
//		textArea.setWidth("200px");
//		textArea.setHeight("200px");
//
//		root.addComponent(button);
//		root.addComponent(button2);
//		root.addComponent(button3);
//		root.addComponent(button4);

//		root.addComponent(textField);
//		root.addComponent(textArea);
	}
}