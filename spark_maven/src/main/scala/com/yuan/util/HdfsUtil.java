package com.yuan.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.yuan.exception.ParameterException;

public class HdfsUtil {

	private final static Log log = LogFactory.getLog(HdfsUtil.class);
	
	/**
	 * 查询HDFS目录下所有文件
	 * @param dst
	 * @param conf
	 * @return
	 * @throws IOException 
	 */
	public static FileStatus[] listStatus(String path,FileSystem fileSystem) throws IOException {
		FileStatus fileList[] = fileSystem.listStatus(new Path(path));
		return fileList;
	}

	/**
	 * 删除文件
	 * @throws IOException 
	 */
	public static void delFile(String direPath,FileSystem fileSystem) throws IOException{
		Path beDeletedPath = new Path(direPath);
		if (fileSystem.exists(beDeletedPath)) {
			if (fileSystem.delete(beDeletedPath, true)) {
				log.info("success to delete the file :"+beDeletedPath.toString());
			} else {
				log.info("failed to delete the file :" + beDeletedPath.toString());
			}
		}
	}
	/**
	 * 创建HDFS文件
	 * @param toCreateFilePath
	 * @param content
	 * @param config
	 * @throws IOException 
	 * @throws Exception 
	 */
	public static void createOrAppendToFile(String toCreateFilePath, String content,FileSystem fSystem) throws IOException {
		Path path = new Path(toCreateFilePath);
		try {
			if(fSystem.exists(path)){
				append(toCreateFilePath, content, fSystem);
			}else{
				write(toCreateFilePath, content, fSystem);
			}
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}
	
	public static void createNewFile(String toCreateFilePath,FileSystem fSystem) throws IOException , ParameterException{
		Path path = new Path(toCreateFilePath);
		if(fSystem.exists(path)){
			throw new ParameterException(toCreateFilePath+" is already exist!");
		}
		fSystem.create(path);
	}

	public static void appendToFile(String toCreateFilePath,String content,FileSystem fSystem) throws Exception{
		Path path = new Path(toCreateFilePath);
		if(!fSystem.exists(path)){
			throw new ParameterException(toCreateFilePath+" is not exist");
		}
		append(toCreateFilePath, content, fSystem);
	}
	
	private static void append(String toCreateFilePath, String content,FileSystem fSystem) throws Exception {
		InputStream in = (InputStream) new ByteArrayInputStream(content.getBytes());
		try {
			HdfsWriter writer = new HdfsWriter(fSystem, toCreateFilePath);
			writer.doAppend(in);
			log.info("success to append to "+toCreateFilePath);
		} finally {
			in.close();
		}
	}

	private static void write(String toCreateFilePath, String content,FileSystem fSystem) throws IOException, ParameterException {
		InputStream in = (InputStream) new ByteArrayInputStream(content.getBytes());
		try {
			HdfsWriter writer = new HdfsWriter(fSystem, toCreateFilePath);
			writer.doWrite(in);
			log.info("success to write."+toCreateFilePath);
		} finally {
			in.close();
		}
	}
}
