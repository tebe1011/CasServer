package de.cas.db;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class H2
 */
@WebServlet("/HSQLDB")
public class EntryPoint extends HttpServlet {
	private static final long serialVersionUID = 1L;
       
	public static Database db;
	
    /**
     * @see HttpServlet#HttpServlet()
     */
    public EntryPoint() {
        super();
        // TODO Auto-generated constructor stub
    }

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void init() throws ServletException {
	    super.init();
	   
	    try {
			db = new Database();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
