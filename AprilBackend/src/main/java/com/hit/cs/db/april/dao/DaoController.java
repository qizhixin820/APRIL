package com.hit.cs.db.april.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.hit.cs.db.april.restresult.Result;

@RestController
@RequestMapping(value = "/dao")
@CrossOrigin(origins = { "*", "null" })
public class DaoController {

	static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
	static final String LUBM_URL = "jdbc:mysql://localhost:3306/testlubm?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
	static final String WATDIV_URL = "jdbc:mysql://localhost:3306/testwatdiv?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
	static final String USER = "root";
	static final String PASS = "goodhigh";

	@GetMapping(value = "/get_all_predicates")
	public Result<String> getAllPredicates() throws Exception {
		Result<String> result = new Result<>();
		Connection conn = null;
		Statement stmt = null;
		result.setMessage("Get all predicates error.");
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(LUBM_URL, USER, PASS);
			stmt = conn.createStatement();
			String sql;
			sql = "SELECT DISTINCT p FROM t0;";
			ResultSet rs = stmt.executeQuery(sql);
			List<String> predicates = new ArrayList<>();
			while (rs.next()) {
				predicates.add(rs.getString("p"));
			}
			rs.close();
			stmt.close();
			conn.close();
			result.setList(predicates);
			result.setStatus("OK");
			result.setMessage("Get all predicates successfully.");
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		return result;
	}

	@GetMapping(value = "/get_cols")
	public Result<String> getCols(String t) throws Exception {
		Result<String> result = new Result<>();
		Connection conn = null;
		Statement stmt = null;
		result.setMessage("Get columns on " + t + " error.");
		try {
			Class.forName(JDBC_DRIVER);
			conn = DriverManager.getConnection(LUBM_URL, USER, PASS);
			stmt = conn.createStatement();
			String sql;
			sql = "DESCRIBE " + t;
			ResultSet rs = stmt.executeQuery(sql);
			List<String> predicates = new ArrayList<>();
			while (rs.next()) {
				predicates.add(rs.getString("Field"));
			}
			rs.close();
			stmt.close();
			conn.close();
			result.setList(predicates);
			result.setStatus("OK");
			result.setMessage("Get columns on " + t + " successfully.");
		} catch (SQLException se) {
			se.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
			}
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException se) {
				se.printStackTrace();
			}
		}
		return result;
	}
}
