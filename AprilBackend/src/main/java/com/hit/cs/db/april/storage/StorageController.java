package com.hit.cs.db.april.storage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;
import com.hit.cs.db.april.restresult.Result;
import com.hit.cs.db.april.Constants;

import queryOptimizer.StarterTDB;

@RestController
@RequestMapping(value = "/storage")
@CrossOrigin(origins = { "*", "null" })
public class StorageController {

	@PostMapping(value = "/start")
	public Result<String> start(HttpServletRequest request) throws Exception {
		Result<String> result = new Result<>();
		String[] choose = request.getParameterValues("choose")[0].split(",");
		/* choose是选项 */
		/* 下面执行start，把执行结果写入到result中，返回之 */
		if (choose[0].equals("0")) { // LUBM
			String[] args = new String[] { Constants.PYTHON_PATH, Constants.PYTHON_PROJECT_DBDQN_SRC + "test_main.py" };
			for (String string : args) {
				System.out.print(string + " ");
			}
			System.out.println();
			//String[] auto = {"D:\\Download\\dbdqn\\dbdqn\\src\\"};
			Process process = Runtime.getRuntime().exec(args,null, new File(Constants.PYTHON_PROJECT_DBDQN_SRC));
			//Process process = Runtime.getRuntime().exec(args);
			// 获取进程的标准输入流
			final InputStream is1 = process.getInputStream();
			// 获取进城的错误流
			final InputStream is2 = process.getErrorStream();
			// 启动两个线程，一个线程负责读标准输出流，另一个负责读标准错误流
			new Thread() {
				public void run() {
					BufferedReader br1 = new BufferedReader(new InputStreamReader(is1));
					try {
						String line1 = null;
						while ((line1 = br1.readLine()) != null) {
							if (line1 != null) {
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						try {
							is1.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}.start();
			new Thread() {
				public void run() {
					BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));
					try {
						String line2 = null;
						while ((line2 = br2.readLine()) != null) {
							if (line2 != null) {
							}
							System.out.println(line2);
							
						}
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						try {
							is2.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}.start();
			int ret = process.waitFor();
			System.out.println("return value:" + ret);
			File file = new File(Constants.PYTHON_PROJECT_DBDQN_LOG + "lubm.txt");
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();
			if (line != null) {
				result.setResult(line.trim());
			}
			result.setStatus("OK");
			result.setMessage("LUBM Storage Selection Done.");
		} else if (choose[0].equals("1")) { // WatDiv
			String[] args = new String[] { Constants.PYTHON_PATH, Constants.PYTHON_PROJECT_DBDQN2_SRC + "test_main.py" };
			for (String string : args) {
				System.out.print(string + " ");
			}
			System.out.println();
			Process process = Runtime.getRuntime().exec(args);
			// 获取进程的标准输入流
			final InputStream is1 = process.getInputStream();
			// 获取进城的错误流
			final InputStream is2 = process.getErrorStream();
			// 启动两个线程，一个线程负责读标准输出流，另一个负责读标准错误流
			new Thread() {
				public void run() {
					BufferedReader br1 = new BufferedReader(new InputStreamReader(is1));
					try {
						String line1 = null;
						while ((line1 = br1.readLine()) != null) {
							if (line1 != null) {
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						try {
							is1.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}.start();
			new Thread() {
				public void run() {
					BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));
					try {
						String line2 = null;
						while ((line2 = br2.readLine()) != null) {
							if (line2 != null) {
							}
						}
					} catch (IOException e) {
						e.printStackTrace();
					} finally {
						try {
							is2.close();
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
				}
			}.start();
			int ret = process.waitFor();
			System.out.println("return value:" + ret);
			File file = new File(Constants.PYTHON_PROJECT_DBDQN2_LOG + "watdiv.txt");
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();
			if (line != null) {
				result.setResult(line.trim());
			}
			result.setStatus("OK");
			result.setMessage("Watdiv Storage Selection Done.");
		}
		return result;
	}

	@PostMapping(value = "/start_user_specific")
	public Result<String> startUserSpecific(HttpServletRequest request) throws Exception {
		Result<String> result = new Result<>();
		result.setMessage("Start user specific failed.");
		String type = request.getParameter("type");
		String userSelectString = request.getParameter("user_select");
		List<String> userSelectList = JSON.parseArray(userSelectString, String.class);
		File argsFile = new File(Constants.APRILBACKEND_RESOURCES_FILE + "contrast_args.txt");
		FileWriter fileWriter = new FileWriter(argsFile);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		for (int i = 0; i < userSelectList.size(); i++) {
			bufferedWriter.write(userSelectList.get(i) + "\n");
		}
		bufferedWriter.close();
		fileWriter.close();
		String[] args = null;
		if (type.equals("lubm")) {
			args = new String[] { Constants.PYTHON_PATH, Constants.PYTHON_PROJECT_DBDQN_SRC + "start_user_specific.py",
					Constants.APRILBACKEND_RESOURCES_FILE + "contrast_args.txt" };
		} else if (type.equals("watdiv")) {
			args = new String[] { Constants.PYTHON_PATH, Constants.PYTHON_PROJECT_DBDQN2_SRC + "start_user_specific.py",
					Constants.APRILBACKEND_RESOURCES_FILE + "contrast_args.txt" };
		} else {
			result.setMessage("Dataset Type Error.");
			return result;
		}
		System.out.println(type);
		for (String string : args) {
			System.out.print(string + " ");
		}
		System.out.println();
		Process process = Runtime.getRuntime().exec(args);
		final InputStream is1 = process.getInputStream();
		final InputStream is2 = process.getErrorStream();
		new Thread() {
			public void run() {
				BufferedReader br1 = new BufferedReader(new InputStreamReader(is1));
				try {
					String line1 = null;
					while ((line1 = br1.readLine()) != null) {
						if (line1 != null) {
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						is1.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
		new Thread() {
			public void run() {
				BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));
				try {
					String line2 = null;
					while ((line2 = br2.readLine()) != null) {
						if (line2 != null) {
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						is2.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
		int ret = process.waitFor();
		System.out.println("return value:" + ret);
		if (type.equals("lubm")) {
			File file = new File(Constants.PYTHON_PROJECT_DBDQN_LOG + "lubm_contrast.txt");
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();
			if (line != null) {
				result.setResult(line.trim());
			}
		} else if (type.equals("watdiv")) {

		}
		result.setStatus("OK");
		result.setMessage("LUBM Storage Selection Done.");
		return result;
	}

	@GetMapping(value = "/save_apply")
	public Result<String> saveApply() throws Exception {
		System.out.println("enter");
		Result<String> result = new Result<>();
		result.setMessage("LUBM Storage Selection Save and Apply Failed.");
		String[] args = new String[] { Constants.PYTHON_PATH, Constants.PYTHON_PROJECT_DBDQN_SRC + "remove_unused_tables.py" };
		for (String string : args) {
			System.out.print(string + " ");
		}
		System.out.println();
		Process process = Runtime.getRuntime().exec(args);
		final InputStream is1 = process.getInputStream();
		final InputStream is2 = process.getErrorStream();
		new Thread() {
			public void run() {
				BufferedReader br1 = new BufferedReader(new InputStreamReader(is1));
				try {
					String line1 = null;
					while ((line1 = br1.readLine()) != null) {
						if (line1 != null) {
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						is1.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
		new Thread() {
			public void run() {
				BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));
				try {
					String line2 = null;
					while ((line2 = br2.readLine()) != null) {
						if (line2 != null) {
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					try {
						is2.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
		int ret = process.waitFor();
		System.out.println("return value:" + ret);
		result.setStatus("OK");
		result.setMessage("LUBM Storage Selection Save and Apply Successfully.");
		return result;
	}
	
	@PostMapping(value = "/get_file_context")
	public Result<String>  getFileContext(HttpServletRequest request) throws Exception {
		Result<String> result = new Result<>();
		
		String resultstr = this._getFileContext(request.getParameter("file_name"));
		
		result.setStatus("OK");
		result.setResult(resultstr);
		return result;
	}
	
	private String _getFileContext(String fileName) throws Exception {
		String resultstr = "";
		File file = new File(fileName);
		if (file.exists()) {
			Long li = file.length();
			byte[] filecontent = new byte[li.intValue()];
			FileInputStream inputs = new FileInputStream(file);
			inputs.read(filecontent);
			inputs.close();
			resultstr = new String(filecontent, "ISO-8859-1");
		}
		return resultstr;
	}
	
	@PostMapping(value = "/query_optimization")
	public Result<String> queryOptimization(HttpServletRequest request) throws Exception {
		Result <String> result = new Result<>();
		
		String querystr = request.getParameter("query");
		String mode = request.getParameter("mode");
		
		List<String> queries = new ArrayList<String>();
		
		String[] querystrs = querystr.split("\\*{10}");
		for (String tmps : querystrs) {
			queries.add(tmps.trim());
		}
		
		String dataset = "TDBLUBM";
		
		String resultTxt;
		
		if (mode.equals("1")) {
			StarterTDB.testDQNQuery(dataset, queries);
			resultTxt = Constants.APRILBACKEND_PROJECT + "dqn_result.txt";
		} else {
			StarterTDB.testGreedyQuery(dataset, queries);
			resultTxt = Constants.APRILBACKEND_PROJECT + "greedy_result.txt";
		}
		
		String resultStr = this._getFileContext(resultTxt);
		
		result.setStatus("OK");
		result.setResult(resultStr);
		return result;
	}
}
