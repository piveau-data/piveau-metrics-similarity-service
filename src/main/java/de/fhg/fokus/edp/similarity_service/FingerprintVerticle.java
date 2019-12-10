package de.fhg.fokus.edp.similarity_service;

import de.fhg.fokus.edp.similarity_service.model.Dataset;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.arq.querybuilder.Order;
import org.apache.jena.arq.querybuilder.SelectBuilder;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.*;
import org.apache.jena.sparql.expr.nodevalue.NodeValueString;
import org.apache.jena.vocabulary.DCAT;
import org.apache.jena.vocabulary.DCTerms;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static de.fhg.fokus.edp.similarity_service.ApplicationConfig.*;

public class FingerprintVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(FingerprintVerticle.class);

    private static final String TMP_SUFFIX = ".tmp";

    private String sparqlUrl;
    private String workDir;

    // stores all possible language codes to allow use of an asterisk in config
    private static final List<String> ALL_LANGUAGE_CODES = Arrays.asList(
        "AUT", "BEL", "BGR", "CHE", "CYP",
        "CZE", "DEU", "DNK", "EST", "ESP",
        "FIN", "FRA", "GBR", "GRC", "HRV",
        "HUN", "IRL", "ISL", "ITA", "LIE",
        "LTU", "LUX", "LV", "MDA", "MLT",
        "NLD", "NOR", "POL", "PRT", "ROU",
        "SRB", "SWE", "SVN", "SVK", "EUROPE");

    /**
     * Number of buckets for TLSH.
     * Originally 256; reduced to account for shorter strings.
     */
    private static final int N_BUCKETS = 64;

    private int[] bucketCount = new int[N_BUCKETS], tmpaux = new int[N_BUCKETS];


    @Override
    public void start(Future<Void> future) {

        sparqlUrl = config().getString(ENV_SPARQL_URL, DEFAULT_SPARQL_URL);
        workDir = config().getString(ENV_WORK_DIR, DEFAULT_WORK_DIR);

        vertx.eventBus().consumer(ADDRESS_START_FINGERPRINT, this::fingerprintLanguages);

        vertx.fileSystem().mkdirs(workDir, mkDirHandler -> {
            if (mkDirHandler.succeeded()) {

                // fingerprint once on startup
                ALL_LANGUAGE_CODES.forEach(this::fingerprint);

                future.complete();
            } else {
                LOG.error("Could not create directory [{}]: {}", workDir, mkDirHandler.cause());
                future.fail("Could not create directory " + workDir);
            }
        });
    }

    private void fingerprintLanguages(Message<String> message) {

        List<String> requestLanguages = new JsonArray(message.body()).getList();

        List<String> languages = requestLanguages.isEmpty() || requestLanguages.contains("*")
            ? ALL_LANGUAGE_CODES
            : requestLanguages;

        languages.forEach(language -> {
            if (ALL_LANGUAGE_CODES.contains(language.toUpperCase())) {
                fingerprint(language);
            } else {
                LOG.error("Unsupported language code [{}]", language);
            }
        });
    }

    /**
     * Main call, usage: java genFP <one or more two-char language codes or __ for pan-European >
     * Not reentrant within same class instance.
     */
    private void fingerprint(String langCode) {
        getCatalogURIs(langCode).setHandler(catalogueHandler ->
            catalogueHandler.result().forEach(catalogueUri -> {
                String catalogueId = StringUtils.substringAfterLast(catalogueUri, "/");
                Path targetFile = Paths.get(workDir).resolve(langCode + "_" + catalogueId + ".fp" + TMP_SUFFIX);

                LOG.debug("Fingerprinting catalogue [{}] to file [{}]", catalogueId, targetFile.toAbsolutePath());
                processCatalogue(catalogueUri, langCode, targetFile, 0, 1024);
            }));

    }

    /**
     * Generate fingerprints for one particular EDP catalog.
     *
     * @param catalogueUri Catalog URI.
     */
    private void processCatalogue(String catalogueUri, String langCode, Path tmpFile, int offset, int limit) {

        getDatasets(catalogueUri, offset, limit).setHandler(handler -> {
            LOG.debug("Retrieved [{}] datasets for catalogue [{}] of language [{}]", handler.result().size(), catalogueUri, langCode);

            int[] fingerprint = new int[N_BUCKETS];

            List<String> fileLines = new ArrayList<>();
            handler.result().forEach(dataset -> {

                String sanitizedTitle = sanitize(dataset.getTitle());
                String sanitizedDescription = sanitize(dataset.getDescription());

                if (!(sanitizedTitle.isEmpty() && sanitizedDescription.isEmpty())) {
                    String fingerprintText = sanitize(dataset.getTitle())
                        + "    "
                        + sanitize(dataset.getDescription());


                    TLSHfingerprint(fingerprintText, fingerprint);

                    StringBuilder stringBuilder = new StringBuilder()
                        .append("\"").append(dataset.getUri()).append("\" \"");

                    for (int i = 0; i < N_BUCKETS; i += 2)
                        stringBuilder.append("0123456789ABCDEF".charAt(fingerprint[i] * 4 + fingerprint[i + 1]));

                    int hflength = dataset.getTitle().length() + dataset.getDescription().length();
                    stringBuilder.append("\" ").append(hflength).append("\n");

                    fileLines.add(stringBuilder.toString());
                }
            });

            vertx.fileSystem().open(tmpFile.toString(), new OpenOptions().setAppend(true), fileHandler -> {
                if (fileHandler.succeeded()) {
                    AsyncFile ws = fileHandler.result();

                    fileLines.forEach(str -> {
                        Buffer chunk = Buffer.buffer(str);
                        ws.write(chunk);
                    });

                    if (handler.result().size() == limit) {
                        // recursively continue processing catalogue
                        processCatalogue(catalogueUri, langCode, tmpFile, offset + limit, limit);
                    } else {
                        // fingerprinting is done, rename file and clean up
                        CopyOptions copyOptions = new CopyOptions()
                            .setAtomicMove(true)
                            .setReplaceExisting(true);

                        String targetFile = StringUtils.removeEnd(tmpFile.toString(), TMP_SUFFIX);

                        vertx.fileSystem().move(tmpFile.toString(), targetFile, copyOptions, moveHandler -> {
                            if (moveHandler.succeeded()) {
                                // trigger reindex of file
                                vertx.eventBus().send(ADDRESS_INDEX_CATALOGUE, targetFile);
                                LOG.info("Finished fingerprinting catalogue [{}]", catalogueUri);
                            } else {
                                LOG.error("Failed to rename fingerprinting temp file [{}] : {}", tmpFile.toAbsolutePath(), moveHandler.cause());
                            }
                        });
                    }
                } else {
                    LOG.error("Could not open file [{}] : {}", tmpFile, fileHandler.cause());
                }
            });
        });
    }

    private String sanitize(String input) {
        return StringUtils.replaceEach( // remove english stop words
            input.replaceAll("(?!\")\\p{Punct}", "") // remove punctuation except double quotes
                .replaceAll("\"", "'") // replace double quotes with single quotes
                .toLowerCase(),
            ENGLISH_STOP_WORDS_L_1,
            Stream.generate(() -> "").limit(ENGLISH_STOP_WORDS_L_1.length).toArray(String[]::new)); // generate array containing the same number of "" as there are stop words
    }

    /**
     * Queries EDP with a single language code, two uppercase chars or empty,
     * and returns a list of the catalogs with that language code
     * (or without language code, resp.).
     * The dct:spatial property of the catalogs is relevant.
     * Typically a handful of catalogs is returned.
     *
     * @param langCode Language code; two Uppercase chars (GB,DE,...); __ or EU for pan-European.
     * @return List of catalog URIs pertaining langCode.
     */
    private Future<List<String>> getCatalogURIs(String langCode) {

        Future<List<String>> completionFuture = Future.future();

        Var catalogue = Var.alloc("catalogue");
        Var spatial = Var.alloc("spatial");

        Query catalogueQuery = new SelectBuilder()
            .addVar(catalogue)
            .addWhere(catalogue, RDF.type, DCAT.Catalog)
            .addOptional(catalogue, DCTerms.spatial, spatial)
            .addFilter(new E_StrContains(new E_Str(new ExprVar(spatial)), new NodeValueString(langCode)))
            .build();

        issueEdpSparqlQuery(catalogueQuery).setHandler(catalogueHandler ->
            completionFuture.complete(catalogueHandler.result().stream()
                .map(solution -> solution.getResource(catalogue.getVarName()).getURI())
                .collect(Collectors.toList())));

        return completionFuture;
    }

    /**
     * builds a SPARQL query
     *
     * @param catalogueUri The EDP data catalog URI whose datasets are to be queried.
     */
    private Future<List<Dataset>> getDatasets(String catalogueUri, int offset, int limit) {

        Future<List<Dataset>> completionFuture = Future.future();

        Var dataset = Var.alloc("dataset");
        Var title = Var.alloc("title");
        Var description = Var.alloc("description");

        Expr languageFilter = new E_LogicalAnd(
            new E_LogicalOr(
                new E_Equals(new E_Lang(new ExprVar(title)), new NodeValueString("")),
                new E_StrStartsWith(new E_Lang(new ExprVar(title)), new NodeValueString("en"))),
            new E_LogicalOr(
                new E_Equals(new E_Lang(new ExprVar(description)), new NodeValueString("")),
                new E_StrStartsWith(new E_Lang(new ExprVar(description)), new NodeValueString("en"))));

        SelectBuilder subQuery = new SelectBuilder()
            .addVar(dataset)
            .addWhere(ResourceFactory.createResource(catalogueUri), DCAT.dataset, dataset)
            .addOrderBy(new ExprVar(dataset), Order.ASCENDING);

        Query datasetQuery = new SelectBuilder()
            .setDistinct(true)
            .addVar(dataset)
            .addVar(title)
            .addVar(description)
            .addSubQuery(subQuery)
            .addWhere(dataset, DCTerms.title, title)
            .addWhere(dataset, DCTerms.description, description)
            .addFilter(languageFilter)
            .setLimit(limit)
            .setOffset(offset)
            .build();

        issueEdpSparqlQuery(datasetQuery).setHandler(catalogueHandler ->
            completionFuture.complete(catalogueHandler.result().stream()
                .map(solution -> new Dataset(
                    solution.getResource(dataset.getVarName()).getURI(),
                    solution.getLiteral(title.getVarName()).getString(),
                    solution.getLiteral(description.getVarName()).getString()
                ))
                .collect(Collectors.toList())));

        return completionFuture;
    }

    /**
     * Submits the query to the SPARQL interface of the EDP.
     * URL encoding of query is taken care of here.
     * In reply returned, header line is NOT skipped.
     *
     * @param query Properly encoded SPARQL query
     * @return unprocessed reply in BufferedReader.
     */
    private Future<List<QuerySolution>> issueEdpSparqlQuery(Query query) {
        LOG.debug("Issuing query [{}] to [{}]", query.toString(), sparqlUrl);

        Future<List<QuerySolution>> completionFuture = Future.future();

        vertx.executeBlocking(queryHandler -> {
                try (QueryExecution queryExecution = QueryExecutionFactory.sparqlService(sparqlUrl, query)) {

                    List<QuerySolution> querySolutions = new ArrayList<>();
                    queryExecution.execSelect().forEachRemaining(querySolutions::add);
                    queryHandler.complete(querySolutions);

                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.error("Failed to issue SPARQL query [{}]: {}", query.toString(), e.getMessage());
                    queryHandler.complete(new ArrayList<>());
                }
            }, resultHandler ->
                completionFuture.complete((List<QuerySolution>) resultHandler.result())
        );

        return completionFuture;
    }

    /**
     * computes TLSH fingerprint of string fingerprintText (which should not be too short)
     * and stores it as values 0..3 in int[N_BUCKETS] fingerprint.
     * Not reentrant in same class instance, due to use of class variables.
     */
    private void TLSHfingerprint(String fingerprintText, int[] fingerprint) throws IllegalArgumentException {
        int i;

        //reset counters:
        for (i = 0; i < N_BUCKETS; i++)
            bucketCount[i] = 0;

        //initialize sliding 5-char window:
        int c0 = (int) fingerprintText.charAt(0), c1 = (int) fingerprintText.charAt(1), c2 = (int) fingerprintText.charAt(2),
            c3 = (int) fingerprintText.charAt(3), c4 = (int) fingerprintText.charAt(4);

        i = 5;

        while (true) {
            bucketCount[pearson3b(c0, c1, c2)]++;
            bucketCount[pearson3b(c0, c3, c1)]++;
            bucketCount[pearson3b(c1, c0, c4)]++;
            bucketCount[pearson3b(c3, c2, c0)]++;
            bucketCount[pearson3b(c4, c0, c2)]++;
            bucketCount[pearson3b(c3, c4, c0)]++;

            //exit when end of string reached:
            if (i >= fingerprintText.length())
                break;

            // slide window forward:
            c0 = c1;
            c1 = c2;
            c2 = c3;
            c3 = c4;
            c4 = (int) fingerprintText.charAt(i);
            i++;
        }

        // do remaining 4 triples:
        bucketCount[pearson3b(c1, c2, c3)]++;
        bucketCount[pearson3b(c1, c4, c2)]++;
        bucketCount[pearson3b(c4, c3, c1)]++;
        bucketCount[pearson3b(c2, c3, c4)]++;

        //sort bucketCount, get quartils:
        System.arraycopy(bucketCount, 0, tmpaux, 0, bucketCount.length);
        Arrays.sort(tmpaux);

        int qutil_1 = tmpaux[(tmpaux.length) / 4],
            qutil_2 = tmpaux[(tmpaux.length) / 2],
            qutil_3 = tmpaux[(3 * tmpaux.length) / 4];

        if (qutil_1 < 0 || qutil_1 > qutil_2 || qutil_2 > qutil_3)
            throw new IllegalArgumentException("Failed to generate TLSH fingerprint. Quartiles inconsistent.");

        //encode :
        for (i = 0; i < N_BUCKETS; i++) {
            int bi = bucketCount[i];
            // ensure 0→0:
            fingerprint[i] = (bi <= qutil_1 ? 0 : bi >= qutil_3 ? 3 : bi >= qutil_2 ? 2 : 1);
        }
    }

    /**
     * Pearson's hash function of three bytes.
     *
     * @return Hash value in range 0..N_BUCKETS - 1.
     */
    private int pearson3b(int pc0, int pc1, int pc2) {
        return (PEARSON_TABLE[PEARSON_TABLE[pc0 & 255] ^ (pc1 & 255)] ^ (pc2 & 255)) % N_BUCKETS;
    }

    /**
     * English stop words, those to be removed from strings before fingerprinting.
     * Only very few, in order not to destroy relevant semantics.
     */
    private static final String[] ENGLISH_STOP_WORDS_L_1 = new String[]{
        "it", "there", "if", "of",
        // conjunctions:
        "and", "so", "yet", "or", "moreover", "also", "too", "thus", "hence", "therefore", "furthermore", "likewise",
        // determiners:
        "a", "an", "the", "other", "another", "some", "any", "its", "their", "such",
        "all", "every", "each",    // but retain one, same, many and most
        //verbs:
        "is", "are", "be", "was", "were", "been", "do", "does", "did", "will", "would",
        // but retain can…, may…, shall…, must
        // foreign:
        "la", "der", "y", "de"
    };


    /**
     * "Random" permutation of bytes, for use in Pearson's hash function.
     */
    private final static int[] PEARSON_TABLE = {
        98, 6, 85, 150, 36, 23, 112, 164, 135, 207, 169, 5, 26, 64, 165, 219, //  1
        61, 20, 68, 89, 130, 63, 52, 102, 24, 229, 132, 245, 80, 216, 195, 115, //  2
        90, 168, 156, 203, 177, 120, 2, 190, 188, 7, 100, 185, 174, 243, 162, 10, //  3
        237, 18, 253, 225, 8, 208, 172, 244, 255, 126, 101, 79, 145, 235, 228, 121, //  4
        123, 251, 67, 250, 161, 0, 107, 97, 241, 111, 181, 82, 249, 33, 69, 55, //  5
        59, 153, 29, 9, 213, 167, 84, 93, 30, 46, 94, 75, 151, 114, 73, 222, //  6
        197, 96, 210, 45, 16, 227, 248, 202, 51, 152, 252, 125, 81, 206, 215, 186, //  7
        39, 158, 178, 187, 131, 136, 1, 49, 50, 17, 141, 91, 47, 129, 60, 99, //  8
        154, 35, 86, 171, 105, 34, 38, 200, 147, 58, 77, 118, 173, 246, 76, 254, //  9
        133, 232, 196, 144, 198, 124, 53, 4, 108, 74, 223, 234, 134, 230, 157, 139, // 10
        189, 205, 199, 128, 176, 19, 211, 236, 127, 192, 231, 70, 233, 88, 146, 44, // 11
        183, 201, 22, 83, 13, 214, 116, 109, 159, 32, 95, 226, 140, 220, 57, 12, // 12
        221, 31, 209, 182, 143, 92, 149, 184, 148, 62, 113, 65, 37, 27, 106, 166, // 13
        3, 14, 204, 72, 21, 41, 56, 66, 28, 193, 40, 217, 25, 54, 179, 117, // 14
        238, 87, 240, 155, 180, 170, 242, 212, 191, 163, 78, 218, 137, 194, 175, 110, // 15
        43, 119, 224, 71, 122, 142, 42, 160, 104, 48, 247, 103, 15, 11, 138, 239  // 16
    };
}
