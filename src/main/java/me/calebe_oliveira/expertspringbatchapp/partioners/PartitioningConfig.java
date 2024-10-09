package me.calebe_oliveira.expertspringbatchapp.partioners;

public class PartitioningConfig {
    private final String[] workerBaseUrls;

    public PartitioningConfig(String[] workerBaseUrls) {
        this.workerBaseUrls = workerBaseUrls;
    }

    public String[] getWorkerBaseUrls() {
        return workerBaseUrls;
    }
}
