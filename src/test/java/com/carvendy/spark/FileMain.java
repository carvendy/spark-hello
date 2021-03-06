package com.carvendy.spark;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

public class FileMain {
	
	public static void main(String[] args) throws IOException {
		File file = new File("/tmp/test-spark-data10.txt");

		char a = 'a';
		Random random = new Random();
		StringBuffer sb  = new StringBuffer();
		for(long i = 0 ; i < 10000*1000;i++){
			char ch = getLetter(a, random);
			
			int count = random.nextInt(2000);
			for(int j =0 ; j < count; j++){
				ch = getLetter(a, random);
				sb = sb.append(ch+",");
			}
			sb = sb.append(ch+"\n");
			if( i % 10000 == 1){
				FileUtils.write(file,sb.toString(),true);
				sb = new StringBuffer();
			}
		}
		System.out.println("over");
	}

	private static char getLetter(char a, Random random) {
		int num = random.nextInt(26);
		char ch = (char)(a + num);
		return ch;
	}
	
}
