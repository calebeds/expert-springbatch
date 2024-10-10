package me.calebe_oliveira.expertspringbatchapp.partioners;

public class PartitioningConfig {
    private final String[] workerBaseUrls;

    public PartitioningConfig(String workerBaseUrlsProperty) {
        workerBaseUrls = workerBaseUrlsProperty.split(",");
    }

    public String[] getWorkerBaseUrls() {
        return workerBaseUrls;
    }
}
