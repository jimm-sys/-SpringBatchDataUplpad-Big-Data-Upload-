package com.batchprocessing.batch.upload;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.springframework.stereotype.Component;
import org.springframework.web.multipart.MultipartFile;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

@Component
public class FileProcessor {

    private static final String CSV = "csv";
    private static final String TXT = "txt";
    private static final String XLSX = "xlsx";

    public List<List<Object[]>> processFile(MultipartFile file, String delimiter, Map<String, String> columnMapping, int chunkSize) throws Exception {
        String extension = getFileExtension(file.getOriginalFilename()).toLowerCase();

        switch (extension) {
            case CSV:
                return processCsvFile(file, columnMapping, chunkSize);
            case TXT:
                return processTxtFile(file, delimiter, columnMapping, chunkSize);
            case XLSX:
                return processXlsxFile(file, columnMapping, chunkSize);
            default:
                throw new IllegalArgumentException("Unsupported file type: " + extension);
        }
    }

    private List<List<Object[]>> processCsvFile(MultipartFile file, Map<String, String> columnMapping, int chunkSize) throws Exception {
        try (CSVReader csvReader = new CSVReader(new InputStreamReader(file.getInputStream()))) {
            return processDelimitedFile(csvReader, columnMapping, chunkSize);
        }
    }

    private List<List<Object[]>> processTxtFile(MultipartFile file, String delimiter, Map<String, String> columnMapping, int chunkSize) throws Exception {
        try (CSVReader csvReader = new CSVReaderBuilder(new InputStreamReader(file.getInputStream()))
                .withCSVParser(new CSVParserBuilder().withSeparator(delimiter.charAt(0)).build())
                .build()) {
            return processDelimitedFile(csvReader, columnMapping, chunkSize);
        }
    }


    private List<List<Object[]>> processDelimitedFile(CSVReader csvReader, Map<String, String> columnMapping, int chunkSize) throws Exception {
        List<List<Object[]>> chunks = new ArrayList<>();
        List<Object[]> currentChunk = new ArrayList<>();

        String[] headers = csvReader.readNext();
        Map<String, Integer> columnIndexMap = getColumnIndexMap(headers, columnMapping);

        String[] line;
        while ((line = csvReader.readNext()) != null) {
            currentChunk.add(mapRow(line, columnIndexMap));
            if (currentChunk.size() >= chunkSize) {
                chunks.add(new ArrayList<>(currentChunk));
                currentChunk.clear();
            }
        }

        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk);
        }
        return chunks;
    }

    private List<List<Object[]>> processXlsxFile(MultipartFile file, Map<String, String> columnMapping, int chunkSize) throws Exception {
        List<List<Object[]>> chunks = new ArrayList<>();
        List<Object[]> currentChunk = new ArrayList<>();

        try (InputStream is = file.getInputStream();
             OPCPackage opcPackage = OPCPackage.open(is)) {
            XSSFReader xssfReader = new XSSFReader(opcPackage);
            SharedStringsTable sst = (SharedStringsTable) xssfReader.getSharedStringsTable();
            XMLStreamReader reader = XMLInputFactory.newInstance().createXMLStreamReader(xssfReader.getSheetsData().next());

            List<String> headers = null;
            Map<String, Integer> columnIndexMap = null;

            while (reader.hasNext()) {
                reader.next();
                if (reader.isStartElement() && "row".equals(reader.getLocalName())) {
                    List<String> rowData = new ArrayList<>();
                    while (reader.hasNext() && !(reader.isEndElement() && "row".equals(reader.getLocalName()))) {
                        reader.next();
                        if (reader.isStartElement() && "c".equals(reader.getLocalName())) {
                            String cellType = reader.getAttributeValue(null, "t");
                            String value = null;
                            while (reader.hasNext() && !(reader.isEndElement() && "c".equals(reader.getLocalName()))) {
                                reader.next();
                                if (reader.isStartElement() && "v".equals(reader.getLocalName())) {
                                    value = reader.getElementText();
                                    break;
                                }
                            }
                            if ("s".equals(cellType) && value != null) {
                                value = sst.getItemAt(Integer.parseInt(value)).getString();
                            }
                            rowData.add(value != null ? value : "");
                        }
                    }
                    if (headers == null) {
                        headers = rowData;
                        columnIndexMap = getColumnIndexMap(headers.toArray(new String[0]), columnMapping);
                    } else {
                        currentChunk.add(mapRow(rowData.toArray(new String[0]), columnIndexMap));
                        if (currentChunk.size() >= chunkSize) {
                            chunks.add(new ArrayList<>(currentChunk));
                            currentChunk.clear();
                        }
                    }
                }
            }
            reader.close();
        }

        if (!currentChunk.isEmpty()) {
            chunks.add(currentChunk);
        }
        return chunks;
    }

    private Map<String, Integer> getColumnIndexMap(String[] headers, Map<String, String> columnMapping) {
        Map<String, Integer> columnIndexMap = new HashMap<>();
        int index = 0;
        for (String header : headers) {
            if (columnMapping.containsKey(header)) {
                columnIndexMap.put(header, index);
            }
            index++;
        }
        return columnIndexMap;
    }

    private Object[] mapRow(String[] values, Map<String, Integer> columnIndexMap) {
        Object[] row = new Object[columnIndexMap.size()];
        int i = 0;
        for (Map.Entry<String, Integer> entry : columnIndexMap.entrySet()) {
            int index = entry.getValue();
            row[i++] = index < values.length ? values[index] : null;
        }
        return row;
    }

    private String getFileExtension(String filename) {
        return filename.substring(filename.lastIndexOf(".") + 1);
    }
}
