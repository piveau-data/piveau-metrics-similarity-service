package de.fhg.fokus.edp.similarity_service;

final class ApplicationConfig {

    static final String ENV_APPLICATION_PORT = "PORT";
    static final Integer DEFAULT_APPLICATION_PORT = 8086;

    static final String ENV_API_KEY = "API_KEY";

    static final String ENV_WORK_DIR = "WORK_DIR";
    static final String DEFAULT_WORK_DIR = "/tmp/dataset-fingerprints/";

    static final String ENV_SPARQL_URL = "SPARQL_URL";
    static final String DEFAULT_SPARQL_URL = "https://www.europeandataportal.eu/sparql";

    static final String ADDRESS_GET_SIMILARITY = "getSimilarity";
    static final String ADDRESS_START_FINGERPRINT = "startFingerprint";
    static final String ADDRESS_INDEX_CATALOGUE = "indexCatalogue";
}
