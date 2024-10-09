package me.calebe_oliveira.expertspringbatchapp.config;

import me.calebe_oliveira.expertspringbatchapp.model.SessionAction;
import me.calebe_oliveira.expertspringbatchapp.model.UserScoreUpdate;
import me.calebe_oliveira.expertspringbatchapp.partioners.HttpRequestPartitionHandler;
import me.calebe_oliveira.expertspringbatchapp.partioners.PartitioningConfig;
import me.calebe_oliveira.expertspringbatchapp.partioners.SessionActionPartitioner;
import me.calebe_oliveira.expertspringbatchapp.utils.SourceDataBaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.TaskExecutorJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.support.builder.SynchronizedItemStreamReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.batch.BatchDataSourceScriptDatabaseInitializer;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class UserActionJobConfiguration {
    private static final Logger LOGGER = LoggerFactory.getLogger(UserActionJobConfiguration.class);

    @Bean
    @Qualifier("simpleActionCalculationJob")
    public AbstractJob simpleActionCalculationJob(JobRepository jobRepository,
                                                  @Qualifier("simpleActionCalculationStep")Step simpleActionCalculationStep) {
        return (AbstractJob)  new JobBuilder("simpleActionCalculationJob", jobRepository)
                .start(simpleActionCalculationStep)
                .build();
    }

    @Bean
    @Qualifier("multiThreadedActionCalculationJob")
    public Job multiThreadedActionCalculationJob(JobRepository jobRepository,
                                                 @Qualifier("multiThreadedActionCalculationStep") Step multiThreadedActionCalculationStep) {
        return new JobBuilder("multiThreadedActionCalculationJob", jobRepository)
                .start(multiThreadedActionCalculationStep)
                .build();
    }

    @Bean
    @Qualifier("partitionedLocalActionCalculationJob")
    public Job paritionedLocalActionCalculationJob(JobRepository jobRepository,
                                                   @Qualifier("partitionedLocalActionCalculationStep") Step partitionedLocalActionCalculationStep) {
        return new JobBuilder("partitionedLocalActionCalculationJob", jobRepository)
                .start(partitionedLocalActionCalculationStep)
                .build();
    }

    @Bean
    @Qualifier("partitionedRemoteActionCalculationJob")
    public Job partitionedRemoteActionCalculationJob(JobRepository jobRepository,
                                                     @Qualifier("partitionedRemoteActionCalculationStep") Step partitionedRemoteActionCalculationStep) {
        return new JobBuilder("partitionedRemoteActionCalculationJob", jobRepository)
                .start(partitionedRemoteActionCalculationStep)
                .build();
    }

    @Bean
    @Qualifier("partitionedRemoteActionCalculationStep")
    public Step partitionedRemoteActionCalculationStep(JobRepository jobRepository, JobExplorer jobExplorer,
                                                       @Qualifier("simpleActionCalculationStep") Step simpleActionCalculationStep,
                                                       PartitioningConfig partitioningConfig) {
        return new StepBuilder("partitionedRemoteActionCalculationStep", jobRepository)
                .partitioner("simpleActionCalculationStep", new SessionActionPartitioner())
                .partitionHandler(new HttpRequestPartitionHandler(simpleActionCalculationStep, partitioningConfig,
                        30000, jobRepository, jobExplorer))
                .build();
    }

    @Bean
    @Qualifier("partitionedLocalActionCalculationStep")
    public Step partitionedLocalActionCalculationStep(JobRepository jobRepository,
                                                      @Qualifier("simpleActionCalculationStep") Step simpleActionCalculationStep) {
        return new StepBuilder("partitionedLocalActionCalculationStep", jobRepository)
                .partitioner("simpleActionCalculationStep", new SessionActionPartitioner())
                .step(simpleActionCalculationStep)
                .taskExecutor(new SimpleAsyncTaskExecutor())
                .gridSize(3)
                .build();
    }

    @Bean
    @Qualifier("multiThreadedActionCalculationStep")
    public Step multiThreadedActionCalculationStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                                   @Qualifier("sessionActionReader") ItemStreamReader<SessionAction> sessionActionReader,
                                                   @Qualifier("sourceDataSource") DataSource sourceDataSource,
                                                   @Qualifier("multiThreadStepExecutor") TaskExecutor multiThreadStepExecutor) {
        return new StepBuilder("multiThreadedActionCalculationStep", jobRepository)
                .<SessionAction, UserScoreUpdate>chunk(5, transactionManager)
                .reader(new SynchronizedItemStreamReaderBuilder<SessionAction>()
                        .delegate(sessionActionReader)
                        .build())
                .processor(getSessionActionProcessor())
                .writer(new JdbcBatchItemWriterBuilder<UserScoreUpdate>()
                        .dataSource(sourceDataSource)
                        .itemPreparedStatementSetter(SourceDataBaseUtils.UPDATE_USER_SCORE_PARAMETER_SETTER)
                        .sql(SourceDataBaseUtils.constructUpdateUserScoreQuery(UserScoreUpdate.USER_SCORE_TABLE_NAME))
                        .build())
                .listener(beforeStepLoggerListener())
                .taskExecutor(multiThreadStepExecutor)
                .build();
    }

    @Bean
    @Qualifier("multiThreadStepExecutor")
    public TaskExecutor multiThreadStepExecutor() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(3);
        return threadPoolTaskExecutor;
    }

    @Bean
    @Qualifier("simpleActionCalculationStep")
    public Step simpleActionCalculationStep(JobRepository jobRepository, PlatformTransactionManager transactionManager,
                                            @Qualifier("sessionActionReader") ItemReader<SessionAction> sessionActionReader,
                                            @Qualifier("sourceDataSource") DataSource sourceDataSource) {
        return new StepBuilder("simpleActionCalculationStep", jobRepository)
                .<SessionAction, UserScoreUpdate>chunk(5, transactionManager)
                .reader(sessionActionReader)
                .processor(getSessionActionProcessor())
                .writer(new JdbcBatchItemWriterBuilder<UserScoreUpdate>()
                        .dataSource(sourceDataSource)
                        .itemPreparedStatementSetter(SourceDataBaseUtils.UPDATE_USER_SCORE_PARAMETER_SETTER)
                        .sql(SourceDataBaseUtils.constructUpdateUserScoreQuery(UserScoreUpdate.USER_SCORE_TABLE_NAME))
                        .build())
                .listener(beforeStepLoggerListener())
                .build();
    }

    private static ItemProcessor<SessionAction, UserScoreUpdate> getSessionActionProcessor() {
        return sessionAction -> {
            if(SourceDataBaseUtils.PLUS_TYPE.equals(sessionAction.getActionType())) {
                return new UserScoreUpdate(sessionAction.getUserId(), sessionAction.getAmount(), 1d);
            } else if(SourceDataBaseUtils.MULTI_TYPE.equals(sessionAction.getActionType())) {
                return new UserScoreUpdate(sessionAction.getUserId(), 0d, sessionAction.getAmount());
            } else {
                throw new RuntimeException("Unknown session action record type: " + sessionAction.getActionType());
            }
        };
    }

    private static StepExecutionListener beforeStepLoggerListener() {
        return new StepExecutionListener() {
            @Override
            public void beforeStep(StepExecution stepExecution) {
                int partitionCount = stepExecution.getExecutionContext().getInt(SessionActionPartitioner.PARTITION_COUNT, -1);
                int partitionIndex = stepExecution.getExecutionContext().getInt(SessionActionPartitioner.PARTITION_INDEX, -1);
                if(partitionIndex == -1 || partitionCount == -1) {
                    LOGGER.info("Calculation step is about to start handling all session action records");
                } else {
                    String threadName = Thread.currentThread().getName();
                    LOGGER.info("Calculation step is about to start handling partition " + partitionIndex +
                            " out of total " + partitionCount + " partitions in the thread -> " + threadName);
                }
            }
        };
    }

    @Bean
    @StepScope
    @Qualifier("sessionActionReader")
    public ItemStreamReader<SessionAction> sessionActionReader(@Qualifier("sourceDataSource") DataSource sourceDataSource,
                                                               @Value("#{stepExecutionContext['partitionCount']}") Integer partitionCount,
                                                               @Value("#{stepExecutionContext['partitionIndex']}") Integer partitionIndex) {
        PagingQueryProvider queryProvider = (partitionCount == null || partitionIndex == null)
                ? SourceDataBaseUtils.selectAllSessionActionsProvider(SessionAction.SESSION_ACTION_TABLE_NAME)
                : SourceDataBaseUtils
                     .selectPartitionOfSessionActionsProvider(SessionAction.SESSION_ACTION_TABLE_NAME, partitionCount, partitionIndex);
        return new JdbcPagingItemReaderBuilder<SessionAction>()
                .name("sessionActionReader")
                .dataSource(sourceDataSource)
                .queryProvider(queryProvider)
                .rowMapper(SourceDataBaseUtils.getSessionActionMapper())
                .pageSize(5)
                .build();
    }

    @Bean
    public PartitioningConfig partitioningConfig(@Value("${worker.server.base.urls}") String workerServerBaseUrls) {
        return new PartitioningConfig(workerServerBaseUrls);
    }

    @Bean
    @Qualifier("asyncJobLauncher")
    public JobLauncher asyncJobLauncher(JobRepository jobRepository) {
        TaskExecutorJobLauncher jobLauncher = new TaskExecutorJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
        return jobLauncher;
    }

    @Bean
    public DataSource dataSource(@Value("${db.url}") String url,
                                 @Value("${db.username}") String username,
                                 @Value("${db.password}") String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);

        return dataSource;
    }

    @Bean
    public PlatformTransactionManager transactionManager(@Qualifier("sourceDataSource") DataSource sourceDataSource) {
        JdbcTransactionManager transactionManager = new JdbcTransactionManager();
        transactionManager.setDataSource(sourceDataSource);
        return transactionManager;
    }

    @Bean
    public BatchDataSourceScriptDatabaseInitializer batchDataSourceInitializer(DataSource dataSource,
                                                                               BatchProperties properties) {
        return new BatchDataSourceScriptDatabaseInitializer(dataSource, properties.getJdbc());
    }

    @Bean
    public BatchProperties batchProperties(@Value("${batch.db.initialize-schema}") DatabaseInitializationMode initializationMode) {
        BatchProperties properties = new BatchProperties();
        properties.getJdbc().setInitializeSchema(initializationMode);
        return properties;
    }
}
