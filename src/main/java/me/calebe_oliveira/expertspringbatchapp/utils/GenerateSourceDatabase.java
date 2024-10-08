package me.calebe_oliveira.expertspringbatchapp.utils;

import me.calebe_oliveira.expertspringbatchapp.config.SourceConfiguration;
import me.calebe_oliveira.expertspringbatchapp.model.SessionAction;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Random;

public class GenerateSourceDatabase {
    private static final int USER_COUNT = 5;
    private static final int RECORD_COUNT = 20;
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(SourceConfiguration.class);
        JdbcTemplate jdbcTemplate = new JdbcTemplate(context.getBean(DataSource.class));

        SourceDataBaseUtils.dropTableIfExists(jdbcTemplate, SessionAction.SESSION_ACTION_TABLE_NAME);
        SourceDataBaseUtils.createSessionActionTable(jdbcTemplate, SessionAction.SESSION_ACTION_TABLE_NAME);

        for (int i = 0; i < RECORD_COUNT; i++) {
            SourceDataBaseUtils.insertSessionAction(jdbcTemplate, generateRecord(i + 1), SessionAction.SESSION_ACTION_TABLE_NAME);
        }

        System.out.println("Input source table with " + RECORD_COUNT + " records is successfully initialized");
    }

    private static SessionAction generateRecord(long id) {
        long userId = 1 + RANDOM.nextInt(USER_COUNT);
        return RANDOM.nextBoolean()
                ? new SessionAction(id, userId, SourceDataBaseUtils.PLUS_TYPE, 1 + RANDOM.nextInt(3))
                : new SessionAction(id, userId, SourceDataBaseUtils.MULTI_TYPE, (double) ((1 + RANDOM.nextInt(5))) / 10 + 1d);
    }
}
