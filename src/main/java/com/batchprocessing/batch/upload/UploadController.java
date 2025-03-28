package com.batchprocessing.batch.upload;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api")
public class UploadController {

    @Autowired
    private DataLoaderService dataLoaderService;

    @PostMapping("/upload")
    public ResponseEntity<String> uploadFile(
            @RequestParam("file") MultipartFile file,
            @RequestParam(value = "delimiter", required = false, defaultValue = ",") String delimiter,
            @RequestParam("columnMapping") String columnMappingJson) {

        try {
            long startTime = System.currentTimeMillis();
            String result = dataLoaderService.processFile(file, delimiter, columnMappingJson);
            long endTime = System.currentTimeMillis();
            double timeTaken = (endTime - startTime) / 1000.0;
            return ResponseEntity.ok(result + " in " + String.format("%.2f", timeTaken) + " seconds");
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("Error: " + e.getMessage());
        }
    }
}
