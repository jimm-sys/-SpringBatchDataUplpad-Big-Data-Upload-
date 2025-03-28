package com.batchprocessing.batch.upload;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class DataLoaderService {

    @Autowired
    private FileProcessor fileProcessor;

    @Autowired
    private TableManager tableManager;

    @Autowired
    private BatchInserter batchInserter;

    @Autowired
    @Qualifier("task")
    private ThreadPoolTaskExecutor taskExecutor;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private static final int CHUNK_SIZE = 100000;

    public String processFile(MultipartFile file, String delimiter, String columnMappingJson) {
        try {
            // Parse column mapping
            Map<String, String> columnMapping = objectMapper.readValue(columnMappingJson, Map.class);
            String tableName = "data_" + defineTableName(file.getOriginalFilename());

            // Check if table already exists
            if (tableManager.tableExists(tableName)) {
                return "Data already loaded into table " + tableName;
            }

            // Create table if not exists
            tableManager.createTable(tableName, columnMapping);

            // Process file and load data asynchronously
            List<List<Object[]>> chunks = fileProcessor.processFile(file, delimiter, columnMapping, CHUNK_SIZE);

            List<CompletableFuture<Void>> futures = chunks.stream()
                    .map(chunk -> CompletableFuture.runAsync(() -> {
                        List<List<String>> stringChunk = chunk.stream()
                                .map(row -> convertRowToStringList(row)) // Convert Object[] to List<String>
                                .toList();
                        batchInserter.insertBatch(tableName, columnMapping.values(), stringChunk);
                    }, taskExecutor))
                    .collect(Collectors.toList());



            // Wait for all batch insert operations to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            return "Data successfully loaded into table " + tableName;
        } catch (Exception e) {
            return "Failed to process file: " + e.getMessage();
        }
    }

    private String defineTableName(String filename) {
        return filename.replaceAll("[^a-zA-Z0-9]", "_");
    }

    private List<String> convertRowToStringList(Object[] row) {
        return Arrays.stream(row)
                .map(obj -> obj != null ? obj.toString() : "") // Convert to String, handle nulls
                .toList();
    }

}
