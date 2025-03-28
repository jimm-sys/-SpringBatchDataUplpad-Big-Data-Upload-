package com.batchprocessing.batch.upload;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

@Component
public class BatchInserter {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public void insertBatch(String tableName, Collection<String> columns, List<List<String>> batch) {
        String sql = "INSERT INTO " + tableName + " (" + String.join(",", columns) + ") VALUES ("
                + String.join(",", Collections.nCopies(columns.size(), "?")) + ")";

        jdbcTemplate.batchUpdate(sql, batch, batch.size(), (ps, row) -> {
            for (int i = 0; i < row.size(); i++) {
                ps.setString(i + 1, row.get(i));
            }
        });
    }


}
