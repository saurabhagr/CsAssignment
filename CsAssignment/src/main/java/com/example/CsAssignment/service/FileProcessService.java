package com.example.CsAssignment.service;

import com.example.CsAssignment.model.FinalObject;
import com.example.CsAssignment.model.InputObject;
import com.example.CsAssignment.model.ObjectStateEnum;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.Scanner;
import java.util.concurrent.*;

import static java.util.Objects.isNull;

@Service
@Slf4j
public class FileProcessService {

    @Value("${queue.size}")
    private int queueSize;
    private ConcurrentHashMap<String, InputObject> map;
    private Scanner scanner;
    private ExecutorService executor;

    private LinkedBlockingQueue<FinalObject> queue;

    public void processFile(String fileName){
        log.info("Processing file [{}]", fileName);
        try {
            scanner = new Scanner(getFileAsIOStream(fileName));
        } catch (Exception e){
            log.error("Exception occurred [{}]", e.getMessage(), e);
            e.printStackTrace();
        }

        executor = Executors.newCachedThreadPool();
        map = new ConcurrentHashMap<>();
        queue = new LinkedBlockingQueue<>(queueSize);
        Thread queueListenerThread = new Thread(this::queueListenerTask, "QueueListenerThread");

        ObjectMapper objectMapper = new ObjectMapper();
        try {
            queueListenerThread.start();
            while (scanner.hasNextLine()) {
                try {
                    InputObject inputObject = objectMapper.readValue(scanner.nextLine(), InputObject.class);
                   // log.info("Processing line [{}]", inputObject);
                    executor.submit(() -> processTask(inputObject.id(), inputObject));
                } catch (JsonProcessingException e) {
                    log.error("Error processing json line [{}]", e.getMessage());
                }
            }
        } finally {
            executor.shutdown();
        }

    }

    private void processTask(String id, InputObject inputObj){
        if(map.containsKey(id)){
            if(ObjectStateEnum.STARTED == inputObj.state()){
                findDurationAndAlertIfApplicable(inputObj, map.remove(id));
            } else {
                findDurationAndAlertIfApplicable(map.remove(id), inputObj);
            }
        } else {
            map.put(id, inputObj);
        }
    }

    private void queueListenerTask(){
        while (true){
            FinalObject obj;
            try {
                obj = queue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            // we can store it in DB or can apply any processing logic here
            if(obj.alert()){
                log.info("FLAGGED EVENT [{}] duration [{}]", obj.id(), obj.duration());
            }
        }
    }

    private void findDurationAndAlertIfApplicable(InputObject started, InputObject finished){
        long duration = finished.timestamp()-started.timestamp();
        boolean alert = duration >= 4;
        FinalObject obj = new FinalObject(started.id(), duration, started.type(), started.host(), alert);
        try {
            queue.put(obj);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    private InputStream getFileAsIOStream(final String fileName)
    {
        InputStream ioStream = this.getClass()
                .getClassLoader()
                .getResourceAsStream(fileName);

        if (isNull(ioStream)) {
            throw new IllegalArgumentException(fileName + " is not found");
        }
        return ioStream;
    }
}
