package com.example.CsAssignment;

import com.example.CsAssignment.service.FileProcessService;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class CsAssignmentApplication implements CommandLineRunner {

	@Autowired
	private FileProcessService fileProcessService;

	public static void main(String[] args) {
		SpringApplication.run(CsAssignmentApplication.class, args);
	}

	@Override
	public void run(String... args) {
		String fileName = "input.txt";
		fileProcessService.processFile(fileName);
		System.exit(0);
	}
}
