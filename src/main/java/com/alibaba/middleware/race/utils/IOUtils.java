package com.alibaba.middleware.race.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class IOUtils {
	public static BufferedReader createReader(String file, int blockSize) throws FileNotFoundException {
		return new BufferedReader(new FileReader(file), blockSize);
	}
	
	public static BufferedWriter createWriter(String file, int blockSize) throws IOException {
		return new BufferedWriter(new FileWriter(file), blockSize);
	}
}
