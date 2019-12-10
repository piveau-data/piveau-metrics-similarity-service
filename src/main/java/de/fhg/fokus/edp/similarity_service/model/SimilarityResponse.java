package de.fhg.fokus.edp.similarity_service.model;

public class SimilarityResponse {

    private String uri;
    private String id;
    private int distance;

    public SimilarityResponse() {
    }

    public SimilarityResponse(String uri, String id, int distance) {
        this.uri = uri;
        this.id = id;
        this.distance = distance;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getDistance() {
        return distance;
    }

    public void setDistance(int distance) {
        this.distance = distance;
    }
}
