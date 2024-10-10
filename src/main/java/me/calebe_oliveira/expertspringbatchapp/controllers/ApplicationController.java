package me.calebe_oliveira.expertspringbatchapp.controllers;

import me.calebe_oliveira.expertspringbatchapp.model.UserScoreUpdate;
import me.calebe_oliveira.expertspringbatchapp.utils.SourceDataBaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.job.AbstractJob;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.sql.DataSource;
import java.util.UUID;

@RestController
public class ApplicationController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationController.class);


    private final JobExplorer jobExplorer;
    private final JobLauncher jobLauncher;
    private final AbstractJob simpleActionCalculationJob;
    private final Job multiThreadedActionCalculationJob;
    private final Job partitionedLocalActionCalculationJob;
    private final Job partitionedRemoteActionCalculationJob;
    private final JobRepository jobRepository;
    private final DataSource sourceDataSource;

    private final TaskExecutor taskExecutor = new SimpleAsyncTaskExecutor();

    public ApplicationController(JobExplorer jobExplorer,
                                 @Qualifier("asyncJobLauncher") JobLauncher jobLauncher,
                                 @Qualifier("simpleActionCalculationJob") AbstractJob simpleActionCalculationJob,
                                 @Qualifier("multiThreadedActionCalculationJob") Job multiThreadedActionCalculationJob,
                                 @Qualifier("partitionedLocalActionCalculationJob") Job partitionedLocalActionCalculationJob,
                                 @Qualifier("partitionedRemoteActionCalculationJob") Job partitionedRemoteActionCalculationJob,
                                 JobRepository jobRepository,
                                 @Qualifier("sourceDataSource") DataSource sourceDataSource) {
        this.jobExplorer = jobExplorer;
        this.jobLauncher = jobLauncher;
        this.simpleActionCalculationJob = simpleActionCalculationJob;
        this.multiThreadedActionCalculationJob = multiThreadedActionCalculationJob;
        this.partitionedLocalActionCalculationJob = partitionedLocalActionCalculationJob;
        this.partitionedRemoteActionCalculationJob = partitionedRemoteActionCalculationJob;
        this.jobRepository = jobRepository;
        this.sourceDataSource = sourceDataSource;
    }

    @PostMapping("/start-simple-local")
    public String startSimpleLocal() throws Exception {
        prepareEmptyResultTable();
        jobLauncher.run(simpleActionCalculationJob, buildUniqueJobParameters());
        return "Successfully started\n";
    }

    @PostMapping("/start-multi-threaded")
    public String startMultiThreaded() throws Exception {
        prepareEmptyResultTable();
        jobLauncher.run(multiThreadedActionCalculationJob, buildUniqueJobParameters());
        return "Successfully started\n";
    }

    @PostMapping("/start-partitioned-local")
    public String startPartitionedLocal() throws Exception {
        prepareEmptyResultTable();
        jobLauncher.run(partitionedLocalActionCalculationJob, buildUniqueJobParameters());
        return "Successfully started\n";
    }

    @PostMapping("/start-partitioned-remote")
    public String startPartitionedRemote() throws Exception {
        prepareEmptyResultTable();
        jobLauncher.run(partitionedRemoteActionCalculationJob, buildUniqueJobParameters());
        return "Successfully started\n";
    }

    @PostMapping("/start-worker")
    public void startWorker(@RequestParam("jobExecutionId") long jobExecutionId,
                            @RequestParam("stepExecutionId") long stepExecutionId,
                            @RequestParam("stepName") String stepName) throws Exception {
        LOGGER.info("Worker endpoint is requested and about to start to execute the partition");
        LOGGER.info("Job execution id: " + jobExecutionId);
        LOGGER.info("Step execution id: " + stepExecutionId);
        LOGGER.info("Step name: " + stepName);

        startWorkerPartitionExecutionAsync(jobExecutionId, stepExecutionId, stepName);
    }

    private void startWorkerPartitionExecutionAsync(long jobExecutionId, long stepExecutionId, String stepName) {
        StepExecution stepExecution = jobExplorer.getStepExecution(jobExecutionId, stepExecutionId);
        if (stepExecution == null) {
            throw new IllegalArgumentException("No step execution exist for job execution id = " + jobExecutionId +
                    " and step execution id = " + stepExecutionId);
        }

        Step step = simpleActionCalculationJob.getStep(stepName);
        if (step == null) {
            throw new IllegalArgumentException("No step with name '" + stepName + "' exist");
        }

        taskExecutor.execute(() -> {
            try {
                step.execute(stepExecution);

                LOGGER.info("Worker successfully completed the execution of the partition");
                LOGGER.info("Job execution id: " + jobExecutionId);
                LOGGER.info("Step execution id: " + stepExecutionId);
                LOGGER.info("Step name: " + stepName);
            } catch (Throwable e) {
                stepExecution.addFailureException(e);
                stepExecution.setStatus(BatchStatus.FAILED);
                jobRepository.update(stepExecution);
            }
        });
    }


    private void prepareEmptyResultTable() {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(sourceDataSource);
        SourceDataBaseUtils.dropTableIfExists(jdbcTemplate, UserScoreUpdate.USER_SCORE_TABLE_NAME);
        SourceDataBaseUtils.createUserScoreTable(jdbcTemplate, UserScoreUpdate.USER_SCORE_TABLE_NAME);
    }

    private JobParameters buildUniqueJobParameters() {
        return new JobParametersBuilder()
                .addString(UUID.randomUUID().toString(), UUID.randomUUID().toString())
                .toJobParameters();
    }
}
