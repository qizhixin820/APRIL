package com.hit.cs.db.april.index;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSON;
import com.hit.cs.db.april.restresult.Result;
import com.hit.cs.db.april.Constants;

@RestController
@RequestMapping(value = "/index")
@CrossOrigin(origins = { "*", "null" })
public class IndexController {

	@PostMapping(value = "/start")
	public Result<String> start(HttpServletRequest request) throws Exception {
		Result<String> result = new Result<>();
		String[] choose = request.getParameterValues("choose")[0].split(",");
		if (choose[0].equals("0")) {
			String[] args = new String[] { Constants.PYTHON_PATH, Constants.PYTHON_PROJECT_DQN_QINDEX_SRC + "tes_dqn.py" };
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
			File file = new File(Constants.PYTHON_PROJECT_DQN_QINDEX_LOG + "lubm.txt");
			FileReader fileReader = new FileReader(file);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			String line = bufferedReader.readLine();
			if (line != null) {
				result.setResult(line.trim());
			}
			result.setStatus("OK");
			result.setMessage("LUBM Storage Selection Done.");
		}
		return result;
	}

	@PostMapping(value = "/start_user_specific")
	public Result<String> startUserSpecific(HttpServletRequest request) throws Exception {
		Result<String> result = new Result<>();
		result.setMessage("Start user specific failed.");
		String userSelectString = request.getParameter("user_select");
		List<String> userSelectList = JSON.parseArray(userSelectString, String.class);
		File argsFile = new File(Constants.APRILBACKEND_RESOURCES_FILE + "index_contrast_args.txt");
		FileWriter fileWriter = new FileWriter(argsFile);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		for (int i = 0; i < userSelectList.size(); i++) {
			bufferedWriter.write(userSelectList.get(i) + "\n");
		}
		bufferedWriter.close();
		fileWriter.close();
		String[] args = new String[] { Constants.PYTHON_PATH, Constants.PYTHON_PROJECT_DQN_QINDEX_SRC + "start_user_specific.py",
				Constants.APRILBACKEND_RESOURCES_FILE + "index_contrast_args.txt" };
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
		File file = new File(Constants.PYTHON_PROJECT_DQN_QINDEX_LOG + "lubm_contrast.txt");
		FileReader fileReader = new FileReader(file);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = bufferedReader.readLine();
		if (line != null) {
			result.setResult(line.trim());
		}
		result.setStatus("OK");
		result.setMessage("LUBM Index Selection Done.");
		return result;
	}
}
