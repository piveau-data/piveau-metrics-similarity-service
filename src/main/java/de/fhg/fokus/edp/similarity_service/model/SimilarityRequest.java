package de.fhg.fokus.edp.similarity_service.model;

public class SimilarityRequest {

    private String datasetId;
    private int limit;

    public SimilarityRequest() {
    }

    public SimilarityRequest(String datasetId, int limit) {
        this.datasetId = datasetId;
        this.limit = limit;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public String toString() {
        return "SimilarityRequest{" +
            "datasetId='" + datasetId + '\'' +
            ", limit=" + limit +
            '}';
    }
}
