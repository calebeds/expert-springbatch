package me.calebe_oliveira.expertspringbatchapp.utils;

import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

public class SourceDataBaseUtils {
    public static final String PLUS_TYPE = "plus";
    public static final String MULTI_TYPE = "multi";

    public static void dropTableIfExists(JdbcTemplate jdbcTemplate, String tableName) {
        jdbcTemplate.update("DROP TABLE IF EXISTS " + tableName);
    }

    public static void createSessionActionTable(JdbcTemplate jdbcTemplate, String tableName) {
        jdbcTemplate.update("CREATE TABLE " + tableName + " (" +
                "id SERIAL PRIMARY KEY," +
                "user_id INT NOT NULL," +
                "action_type VARCHAR(36) NOT NULL," +
                "amount NUMERIC(10,2) NOT NULL" +
                ")");
    }

    public static void createUserScoreTable(JdbcTemplate jdbcTemplate, String tableName) {
        jdbcTemplate.update("CREATE TABLE " + tableName + " (" +
                "user_id INT NOT NULL UNIQUE," +
                "score NUMERIC(10,2) NOT NULL" +
                ")");
    }

    public static String constructUpdateUserScoreQuery(String tableName) {
        return "INSERT INTO " + tableName + " (user_id, score) VALUES (?, ?) " +
                "ON CONFLICT (user_id) DO " +
                "UPDATE SET SCORE = " + tableName + ".score * ? + ?";
    }


}
