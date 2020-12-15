package com.hit.cs.db.april.upload;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import com.hit.cs.db.april.restresult.Result;

@RestController
@RequestMapping(value = "/upload")
@CrossOrigin(origins = { "*", "null" })
public class DataController {

	@PostMapping(value = "/upload")
	public Result<String> upload(HttpServletRequest request) throws Exception {
		// 实际上每次传进来的文件只有一个
		Result<String> result = new Result<>();
		MultipartHttpServletRequest params = ((MultipartHttpServletRequest) request);
		List<MultipartFile> files = params.getFiles("files[]");
		String type = params.getParameter("type");
		if (type.equals("0")) {
			for (int i = 0; i < files.size(); i++) {
				if (!files.get(i).isEmpty()) {
					System.out.println(files.get(i).getOriginalFilename());
					try (FileOutputStream fileOuputStream = new FileOutputStream(
							"D:\\DriveY\\Spring\\AprilBackend\\src\\main\\resources\\file\\"
									+ files.get(i).getOriginalFilename())) {
						fileOuputStream.write(files.get(i).getBytes());
					} catch (IOException e) {
						e.printStackTrace();
					}
					/* 导入进入数据库。。。 */
					String[] database = new String[] { "testlubm", "priority", "contrast" };
					for (String db : database) {
						String[] args = new String[] { "D:\\MyEnv\\Anaconda\\envs\\ml\\python",
								"D:\\Download\\dbdqn\\dbdqn\\src\\init_lubm_data.py",
								"D:\\DriveY\\Spring\\AprilBackend\\src\\main\\resources\\file\\"
										+ files.get(i).getOriginalFilename(),
								db };
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
					}
				}
			}
			result.setStatus("OK");
			result.setMessage("Upload Data Successfully.");
		} else if (type.equals("1")) {
			for (int i = 0; i < files.size(); i++) {
				if (!files.get(i).isEmpty()) {
					System.out.println(files.get(i).getOriginalFilename());
					try (FileOutputStream fileOuputStream = new FileOutputStream(
							"D:\\DriveY\\Spring\\AprilBackend\\src\\main\\resources\\file\\"
									+ files.get(i).getOriginalFilename())) {
						fileOuputStream.write(files.get(i).getBytes());
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			result.setStatus("OK");
			result.setMessage("Upload Workload Successfully.");
		} else {
			result.setStatus("ERROR");
			result.setMessage("Upload type error.");
		}
		return result;
	}
}
