package com.batchprocessing.batch.upload;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class TableManager {

     @Autowired
     private JdbcTemplate jdbcTemplate;

     //check if the table exist
     public boolean tableExists(String tableName) {
          String sql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?";
          Integer count = jdbcTemplate.queryForObject(sql, Integer.class, tableName);
          return count != null && count > 0;
     }

     public void createTable(String tableName, Map<String, String> columnMapping) {
          StringBuilder sql = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");
          for (String dbColumn : columnMapping.values()) {
               sql.append(dbColumn).append(" VARCHAR(255), ");
          }
          if (columnMapping.size() > 0) {
               sql.setLength(sql.length() - 2);
          }
          sql.append(")");
          jdbcTemplate.execute(sql.toString());
     }
}


