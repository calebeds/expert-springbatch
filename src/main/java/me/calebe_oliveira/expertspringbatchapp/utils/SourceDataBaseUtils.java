package me.calebe_oliveira.expertspringbatchapp.utils;

import me.calebe_oliveira.expertspringbatchapp.model.SessionAction;
import me.calebe_oliveira.expertspringbatchapp.model.UserScoreUpdate;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.util.Collections;

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

    public static ItemPreparedStatementSetter<UserScoreUpdate> UPDATE_USER_SCORE_PARAMETER_SETTER = ((item, ps) -> {
        ps.setLong(1, item.getUserId());
        ps.setDouble(2, item.getAdd());
        ps.setDouble(3,item.getMultiply());
        ps.setDouble(4, item.getAdd());
    });

    public static void insertSessionAction(JdbcTemplate jdbcTemplate, SessionAction sessionAction, String tableName) {
        jdbcTemplate
                .update("INSERT INTO " + tableName + " (id, user_id, action_type, amount) VALUES (?, ?, ?, ?)",
                        sessionAction.getId(), sessionAction.getUserId(), sessionAction.getActionType(), sessionAction.getAmount());
    }

    public static PostgresPagingQueryProvider selectAllSessionActionsProvider(String tableName) {
        PostgresPagingQueryProvider queryProvider = new PostgresPagingQueryProvider();
        queryProvider.setSelectClause("id, user_id, action_type, amount");
        queryProvider.setFromClause(tableName);
        queryProvider.setSortKeys(Collections.singletonMap("id", Order.ASCENDING));
        return queryProvider;
    }

    public static PagingQueryProvider selectPartitionOfSessionActionsProvider(String tableName,
                                                                              int partitionCount, int partitionIndex) {
        PostgresPagingQueryProvider queryProvider = selectAllSessionActionsProvider(tableName);
        queryProvider.setWhereClause("user_id % " + partitionCount + " = " + partitionIndex);
        return queryProvider;
    }

    public static RowMapper<SessionAction> getSessionActionMapper() {
        return ((rs, rowNum) ->
                new SessionAction(rs.getLong("id"), rs.getLong("user_id"),
                        rs.getString("action_type"), rs.getDouble("amount")));
    }
}
