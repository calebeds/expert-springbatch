package me.calebe_oliveira.expertspringbatchapp.partioners;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.partition.support.AbstractPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;


import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class HttpRequestPartitionHandler extends AbstractPartitionHandler {

    private final Step workerStep;
    private final PartitioningConfig partitioningConfig;
    private final long endToEndTimeoutMillis;
    private final JobRepository jobRepository;
    private final JobExplorer jobExplorer;

    private static final CloseableHttpClient HTTP_CLIENT = HttpClientBuilder.create().build();
    private static final String START_WORKER_ENDPOINT = "start-worker";
    private static final long NANO_IN_MILLI = 1000000;

    public HttpRequestPartitionHandler(Step workerStep, PartitioningConfig partitioningConfig, long endToEndTimeoutMillis,
                                       JobRepository jobRepository, JobExplorer jobExplorer) {
        this.workerStep = workerStep;
        this.partitioningConfig = partitioningConfig;
        this.endToEndTimeoutMillis = endToEndTimeoutMillis;
        this.jobRepository = jobRepository;
        this.jobExplorer = jobExplorer;
        this.gridSize = partitioningConfig.getWorkerBaseUrls().length;
    }

    protected Set<StepExecution> doHandle(StepExecution managerStepExecution, Set<StepExecution> partitionStepExecutions) throws Exception {
        if (partitioningConfig.getWorkerBaseUrls().length != partitionStepExecutions.size()) {
            throw new IllegalArgumentException("Misconfiguration: grid size " + partitionStepExecutions.size() +  " should be equal to the number of workers " + partitioningConfig.getWorkerBaseUrls().length + "  provided through partitioning config");
        }

        Iterator<StepExecution> stepExecutionIterator = partitionStepExecutions.iterator();
        for (String workerBaseUrl : partitioningConfig.getWorkerBaseUrls()) {
            sendStartWorkerRequest(workerBaseUrl, stepExecutionIterator.next());
        }

        long startTime = System.nanoTime();
        while ((System.nanoTime() - startTime) / NANO_IN_MILLI < endToEndTimeoutMillis) {
            if (checkPartitionStepExecutionCompleted(managerStepExecution, partitionStepExecutions)) {
                return partitionStepExecutions;
            } else {
                Thread.sleep(1000);
            }
        }
        throw new RuntimeException("HTTP request partition handler timed out");
    }

    private void sendStartWorkerRequest(String workerBaseUrl, StepExecution partitionStepExecution) throws Exception {
        URI uri = new URIBuilder(workerBaseUrl + START_WORKER_ENDPOINT)
                .addParameter("jobExecutionId", Long.toString(partitionStepExecution.getJobExecutionId()))
                .addParameter("stepExecutionId", Long.toString(partitionStepExecution.getId()))
                .addParameter("stepName", workerStep.getName())
                .build();
        CloseableHttpResponse response = HTTP_CLIENT.execute(new HttpPost(uri));
        if (response.getStatusLine().getStatusCode() != 200) {
            ExitStatus exitStatus = ExitStatus.FAILED
                    .addExitDescription("HTTP request to start worker did not finish successfully, so exiting");
            partitionStepExecution.setStatus(BatchStatus.FAILED);
            partitionStepExecution.setExitStatus(exitStatus);
            jobRepository.update(partitionStepExecution);
        }
    }

    private boolean checkPartitionStepExecutionCompleted(StepExecution managerStepExecution, Set<StepExecution> partitionStepExecutions) {
        JobExecution jobExecution = jobExplorer.getJobExecution(managerStepExecution.getJobExecutionId());
        Map<Long, BatchStatus> stepExecutionStatusMap = new HashMap<>(jobExecution.getStepExecutions().size());
        for (StepExecution queriedStepExecution : jobExecution.getStepExecutions()) {
            stepExecutionStatusMap.put(queriedStepExecution.getId(), queriedStepExecution.getStatus());
        }

        boolean jobComplete = true;
        for (StepExecution partitionStepExecution : partitionStepExecutions) {
            BatchStatus partitionStepStatus = stepExecutionStatusMap.get(partitionStepExecution.getId());
            partitionStepExecution.setStatus(partitionStepStatus); // Also, setting proper status such that it could be used upstream
            if (!BatchStatus.COMPLETED.equals(partitionStepStatus) && !BatchStatus.FAILED.equals(partitionStepStatus)) {
                jobComplete = false;
            }
        }
        return jobComplete;
    }
}
