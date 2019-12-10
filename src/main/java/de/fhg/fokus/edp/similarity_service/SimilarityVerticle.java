package de.fhg.fokus.edp.similarity_service;

import de.fhg.fokus.edp.similarity_service.model.SimilarityRequest;
import de.fhg.fokus.edp.similarity_service.model.SimilarityResponse;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static de.fhg.fokus.edp.similarity_service.ApplicationConfig.*;

public class SimilarityVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(SimilarityVerticle.class);

    /**
     * Each byte of a TLSH fingerprint contain 4 2-bit values.
     * The difference between two such bytes x,y
     * is rapidly determined by diffCount[(x^y)&255].
     * Initialization in constructor; no alterations afterwards.
     */
    private int[] diffCount = new int[256];

    /**
     * Assigns TLSH fingerprint and length of title+description to every URI.
     * Initialization in constructor; no alterations afterwards.
     */
    private HashMap<String, WithIntAttr<byte[]>> fingerprint = new HashMap<>();

    private final int unitDist = 8;

    private String sparqlUrl;


    @Override
    public void start(Future<Void> future) {

        vertx.eventBus().consumer(ADDRESS_GET_SIMILARITY, this::getSimilarity);
        vertx.eventBus().consumer(ADDRESS_INDEX_CATALOGUE, this::handleIndexRequest);

        sparqlUrl = config().getString(ENV_SPARQL_URL, DEFAULT_SPARQL_URL);

        // initialize diffCount array:
        diffCount[0] = 0;
        int length = 1;
        do {
            for (int i = 0; i < length; i++) {
                //differentiate a bit:
                diffCount[i + length] = diffCount[i + 3 * length] = diffCount[i] + unitDist;
                diffCount[i + 2 * length] = diffCount[i] + unitDist + 2;
            }
            length *= 4;
        } while (length < 256);

        future.complete();
    }

    /**
     * Compare title+description of one dataset in EDP with all others.
     *
     * @return Json array with hits up to distance 40, in ascending order.
     */
    private void getSimilarity(Message<String> message) {

        SimilarityRequest request = Json.decodeValue(message.body(), SimilarityRequest.class);
        LOG.debug("Received {}", request);

        String datasetUri = "https://europeandataportal.eu/set/data/" + request.getDatasetId();
        List<SimilarityResponse> similarities = new ArrayList<>();
        WithIntAttr<byte[]> comp = fingerprint.get(datasetUri);

        if (comp != null) {

            byte[] compLeft = comp.getVal();
            ArrayList<WithIntAttr<String>> result = new ArrayList<>();

            // Compare <datasetUri> against all other datasets,
            // collecting results in ArrayList listRes.
            // (following loop needs to be fast):
            for (Map.Entry<String, WithIntAttr<byte[]>> curr : fingerprint.entrySet()) {
                byte[] compRight = curr.getValue().getVal();
                int distance
                    = diffCount[(compLeft[0] ^ compRight[0]) & 255]
                    + diffCount[(compLeft[1] ^ compRight[1]) & 255]
                    + diffCount[(compLeft[2] ^ compRight[2]) & 255]
                    + diffCount[(compLeft[3] ^ compRight[3]) & 255]
                    + diffCount[(compLeft[4] ^ compRight[4]) & 255]
                    + diffCount[(compLeft[5] ^ compRight[5]) & 255]
                    + diffCount[(compLeft[6] ^ compRight[6]) & 255]
                    + diffCount[(compLeft[7] ^ compRight[7]) & 255]
                    + diffCount[(compLeft[8] ^ compRight[8]) & 255]
                    + diffCount[(compLeft[9] ^ compRight[9]) & 255]
                    + diffCount[(compLeft[10] ^ compRight[10]) & 255]
                    + diffCount[(compLeft[11] ^ compRight[11]) & 255]
                    + diffCount[(compLeft[12] ^ compRight[12]) & 255]
                    + diffCount[(compLeft[13] ^ compRight[13]) & 255]
                    + diffCount[(compLeft[14] ^ compRight[14]) & 255]
                    + diffCount[(compLeft[15] ^ compRight[15]) & 255];
                distance /= unitDist;    // cf. diffCount initialization in constructor

                // incorporate length comparison (because basic TLSH fingerprinting is length-agnostic):
                double lngLeft = comp.getAttr(), lngRight = curr.getValue().getAttr();
                double x = Math.abs(lngLeft - lngRight) / Math.max(lngLeft, lngRight);

                // now x is between 0.0 and 1.0 inclusively; polynomial weighting follows:
                distance += (int) (48.0 * x * x * (-2.0 * x + 3.0));
                if (distance <= 40 && !curr.getKey().equals(datasetUri)) // ... compare against all *other* datasets ...
                    result.add(new WithIntAttr<>(curr.getKey(), distance));
            }

            // sort results in ascending distance:
            Collections.sort(result);

            // only return list of IDs instead of entire URI
            similarities.addAll(result.stream()
                .limit(request.getLimit() > 0 ? request.getLimit() : fingerprint.size())
                .map(curr ->
                    new SimilarityResponse(curr.getVal(), StringUtils.substringAfterLast(curr.getVal(), "/"), curr.getAttr()))
                .collect(Collectors.toList()));

        } else {
            LOG.debug("Could not find fingerprint for URI " + datasetUri);
        }

        message.reply(Json.encode(similarities));
    }

    private void handleIndexRequest(Message<String> message) {
        vertx.fileSystem().exists(message.body(), existsHandler -> {
            if (existsHandler.succeeded() && existsHandler.result()) {
                indexFingerprintFile(Paths.get(message.body()));
            } else {
                LOG.error("Fingerprint file [{}] does not exist", message.body());
            }
        });
    }

    private void indexFingerprintFile(Path fingerprintFile) {
        try (InputStreamReader sr = new InputStreamReader(new FileInputStream(fingerprintFile.toFile()))) {
            StreamTokenizer tok = new StreamTokenizer(sr);
            tok.quoteChar('"');
            tok.parseNumbers();    //??
            tok.eolIsSignificant(false);

            while (tok.nextToken() != StreamTokenizer.TT_EOF) {
                if (tok.ttype != '"') {
                    LOG.error("URI string expected in file [{}]", fingerprintFile.getFileName());
                    break;
                }

                String uri = tok.sval;
                tok.nextToken();

                if (tok.ttype != '"') {
                    LOG.error("Fingerprint string expected in file [{}]", fingerprintFile.getFileName());
                    break;
                }

                if (tok.sval.length() != 32) {
                    LOG.error("32 hex digits expected in file [{}]", fingerprintFile.getFileName());
                    break;
                }

                byte[] bytes = DatatypeConverter.parseHexBinary(tok.sval);

                if (tok.nextToken() != StreamTokenizer.TT_NUMBER) {
                    LOG.error("Number expected in file [{}]", fingerprintFile.getFileName());
                    break;
                }

                int textLength = (int) tok.nval;

                fingerprint.put(uri, new WithIntAttr<>(bytes, textLength));
            }
            LOG.debug("Successfully (re)loaded file [{}]", fingerprintFile.getFileName());
        } catch (IOException e) {
            LOG.error("Failed to read File [{}]", fingerprintFile.getFileName(), e);
        }
    }


    /**
     * One-parameter generic class with two immutable fields,
     * one of the generic parameter type,
     * the other one of type integer.
     */
    private class WithIntAttr<T> implements Comparable<WithIntAttr<T>> {
        private T val;
        private int attr;

        WithIntAttr(T pk, int pa) {
            val = pk;
            attr = pa;
        }

        T getVal() {
            return val;
        }

        int getAttr() {
            return attr;
        }

        @Override
        public int compareTo(WithIntAttr<T> other) {
            return attr - ((other).getAttr());
        }
    }
}
