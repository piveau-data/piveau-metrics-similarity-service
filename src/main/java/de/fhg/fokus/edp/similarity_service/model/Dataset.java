package de.fhg.fokus.edp.similarity_service.model;

public class Dataset {

    private String uri;
    private String title;
    private String description;

    public Dataset(String uri, String title, String description) {
        this.uri = uri;
        this.title = title;
        this.description = description;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
