package com.batchprocessing.batch.upload;

import java.util.List;
import java.util.Map;

public interface FileHandler {
    List<Map<String, Object>> processFile(String filePath, String delimiter) throws Exception;
}
