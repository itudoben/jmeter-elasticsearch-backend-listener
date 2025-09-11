package io.github.delirius325.jmeter.backendlistener.elasticsearch;

import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;

import static io.github.delirius325.jmeter.backendlistener.elasticsearch.ElasticSearchRequests.SEND_BULK_REQUEST;

public class ElasticSearchMetricSender {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchMetricSender.class);
    private static final int BULK_ERROR_DETAILS_LIMIT = 5;
    private static final int OPENSEARCH_ES_COMPAT_MAJOR = 7;
    private final RestClient client;
    private final String esIndex;
    private final List<String> metricList;
    private final String authUser;
    private final String authPwd;
    private final String awsEndpoint;

    public ElasticSearchMetricSender(RestClient cli, String index, String user, String pwd, String endpoint) {
        this.client = cli;
        this.esIndex = index;
        this.metricList = new LinkedList<String>();
        this.authUser = user.trim();
        this.authPwd = pwd.trim();
        this.awsEndpoint = endpoint;
    }

    /**
     * This method returns the current size of the ElasticSearch documents list
     *
     * @return integer representing the size of the ElasticSearch documents list
     */
    public int getListSize() {
        return this.metricList.size();
    }

    /**
     * This method closes the REST client
     */
    public void closeConnection() throws IOException {
        this.client.close();
    }

    /**
     * This method clears the ElasticSearch documents list
     */
    public void clearList() {
        this.metricList.clear();
    }

    /**
     * This method adds a metric to the list (metricList).
     *
     * @param metric String parameter representing a JSON document for ElasticSearch
     */
    public void addToList(String metric) {
        this.metricList.add(metric);
    }

    /**
     * This method sets the Basic Authorization header to requests
     */
    private Request setAuthorizationHeader(Request request) {
        if (this.awsEndpoint.equals("") && !this.authPwd.equals("")) {
            String encodedCredentials = Base64.getEncoder()
                    .encodeToString((this.authUser + ":" + this.authPwd).getBytes());
            RequestOptions.Builder options = request.getOptions().toBuilder();
            options.addHeader("Authorization", "Basic " + encodedCredentials);
            request.setOptions(options);

        }
        return request;
    }

    /**
     * This method creates the ElasticSearch index.
     */
    public void createIndex() {
        try {
            this.client.performRequest(setAuthorizationHeader(new Request("PUT", "/" + this.esIndex)));
        } catch (IOException e) {
            logger.info("Index already exists!");
        }
    }

    public int getElasticSearchVersion() {
        Request request = new Request("GET", "/");
        int elasticSearchVersion = -1;
        try {
            logger.info("Sending the GET request to get the version of the Elasticsearch platform for hosts {}", getTargetHosts());
            Response response = this.client.performRequest(setAuthorizationHeader(request));
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK && logger.isErrorEnabled()) {
                logger.error("Unable to perform request to ElasticSearch engine for index {}. Response status: {}",
                        this.esIndex, response.getStatusLine().toString());
            } else {
                String responseBody = EntityUtils.toString(response.getEntity());
                JSONObject elasticSearchConfig = new JSONObject(responseBody);
                elasticSearchVersion = resolveElasticSearchCompatibilityMajor(elasticSearchConfig);
                logger.debug("ElasticSearch compatibility major version: {}", elasticSearchVersion);
            }
            logger.info("ElasticSearch Backend Listener has successfully performed GET request to ES instance hosts [{}] " +
                            "to get the version of the platform",
                    getTargetHosts());
        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Exception" + e);
                logger.error("ElasticSearch Backend Listener was unable to perform request to the ElasticSearch engine. " +
                        "Check your JMeter console for more info.");
            }
        }
        return elasticSearchVersion;
    }

    private int resolveElasticSearchCompatibilityMajor(JSONObject elasticSearchConfig) {
        JSONObject version = elasticSearchConfig.optJSONObject("version");
        if (version == null) {
            return -1;
        }

        String distribution = version.optString("distribution", "");
        String versionNumber = version.optString("number", "");
        String tagline = elasticSearchConfig.optString("tagline", "");

        boolean isOpenSearch = "opensearch".equalsIgnoreCase(distribution)
                || tagline.toLowerCase().contains("opensearch");

        if (isOpenSearch) {
            logger.info("Detected OpenSearch distribution (reported version {}). Mapping to Elasticsearch compatibility major {}",
                    versionNumber, OPENSEARCH_ES_COMPAT_MAJOR);
            return OPENSEARCH_ES_COMPAT_MAJOR;
        }

        return parseMajorVersion(versionNumber);
    }

    private int parseMajorVersion(String versionNumber) {
        if (versionNumber == null || versionNumber.trim().isEmpty()) {
            return -1;
        }

        try {
            return Integer.parseInt(versionNumber.split("\\.")[0]);
        } catch (NumberFormatException ex) {
            logger.warn("Unable to parse Elasticsearch version number '{}'", versionNumber);
            return -1;
        }
    }

    private String getTargetHosts() {
        if (client.getNodes() == null || client.getNodes().isEmpty()) {
            return "unknown";
        }

        List<String> hosts = new ArrayList<String>();
        for (org.elasticsearch.client.Node node : client.getNodes()) {
            hosts.add(node.getHost().toHostString());
        }
        return String.join(",", hosts);
    }

    /**
     * This method sends the ElasticSearch documents for each document present in the list (metricList). All is being
     * sent through the low-level ElasticSearch REST Client.
     */
    public void sendRequest(int elasticSearchVersionPrefix) {
        Request request;
        StringBuilder bulkRequestBody = new StringBuilder();
        String actionMetaData;
        if (elasticSearchVersionPrefix < 7) {
            request = new Request("POST", "/" + this.esIndex + "/SampleResult/_bulk");
            actionMetaData = String.format(SEND_BULK_REQUEST, this.esIndex, "SampleResult");
        } else {
            request = new Request("POST", "/" + this.esIndex + "/_bulk");
            actionMetaData = String.format(SEND_BULK_REQUEST, this.esIndex);
        }

        for (String metric : this.metricList) {
            bulkRequestBody.append(actionMetaData);
            bulkRequestBody.append(metric);
            bulkRequestBody.append("\n");
        }
        logger.debug("Metrics sent: {}", metricList);
        logger.debug("bulkRequestBody sent: {}",
                bulkRequestBody.substring(0, Math.min(bulkRequestBody.length(), 1000)) +
                        (bulkRequestBody.length() > 1000 ? "...(truncated)" : ""));

        request.setEntity(new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON));

        try {
            Response response = this.client.performRequest(setAuthorizationHeader(request));
            int statusCode = response.getStatusLine().getStatusCode();
            String responseBody = response.getEntity() == null ? "" : EntityUtils.toString(response.getEntity());

            if (statusCode != HttpStatus.SC_OK) {
                if (logger.isErrorEnabled()) {
                    logger.error("ElasticSearch Backend Listener failed to write results for index {}. Response status: {}",
                            this.esIndex, response.getStatusLine().toString());
                }
            } else {
                BulkResponseValidation validation = validateBulkResponse(responseBody);
                if (validation.hasFailures()) {
                    logger.error("ElasticSearch Backend Listener bulk request reported failures for index {}. " +
                                    "Failed items: {}/{}. Top-level errors flag: {}. Details: {}",
                            this.esIndex,
                            validation.failedItems,
                            validation.totalItems,
                            validation.topLevelErrors,
                            validation.failureDetails);
                } else if (logger.isInfoEnabled()) {
                    logger.info("ElasticSearch Backend Listener has successfully written {} documents to ES instance [{}]",
                            validation.totalItems,
                            client.getNodes().iterator().next().getHost().toHostString());
                }
            }

        } catch (Exception e) {
            if (logger.isErrorEnabled()) {
                logger.error("Exception" + e);
                logger.error("Exception while sending request to ElasticSearch engine for index {}. Endpoint: {}",
                        this.esIndex, request.getEndpoint(), e);
                logger.error("ElasticSearch Backend Listener was unable to perform request to the ElasticSearch engine. " +
                        "Check your JMeter console for more info.");
            }
        }
    }

    static BulkResponseValidation validateBulkResponse(String responseBody) {
        if (responseBody == null || responseBody.trim().isEmpty()) {
            List<String> details = new ArrayList<String>();
            details.add("Bulk response body is empty");
            return new BulkResponseValidation(true, 0, 0, details);
        }

        try {
            JSONObject bulkResponse = new JSONObject(responseBody);
            boolean topLevelErrors = bulkResponse.optBoolean("errors", false);
            JSONArray items = bulkResponse.optJSONArray("items");

            int totalItems = items == null ? 0 : items.length();
            int failedItems = 0;
            List<String> failureDetails = new ArrayList<String>();

            if (items != null) {
                for (int i = 0; i < items.length(); i++) {
                    JSONObject itemWrapper = items.optJSONObject(i);
                    if (itemWrapper == null || !itemWrapper.keys().hasNext()) {
                        failedItems++;
                        addFailureDetail(failureDetails, String.format("Item %d has invalid bulk item shape", i));
                        continue;
                    }

                    String action = itemWrapper.keys().next();
                    JSONObject actionResult = itemWrapper.optJSONObject(action);
                    if (actionResult == null) {
                        failedItems++;
                        addFailureDetail(failureDetails, String.format("Item %d action '%s' has no object response", i, action));
                        continue;
                    }

                    int itemStatus = actionResult.optInt("status", -1);
                    Object error = actionResult.opt("error");
                    boolean hasItemError = error != null && !JSONObject.NULL.equals(error);
                    boolean failedByStatus = itemStatus < 0 || itemStatus >= 300;

                    if (hasItemError || failedByStatus) {
                        failedItems++;
                        String reason;
                        if (hasItemError) {
                            reason = error.toString();
                        } else {
                            reason = String.format("status=%d", itemStatus);
                        }
                        String id = actionResult.optString("_id", "unknown");
                        addFailureDetail(failureDetails,
                                String.format("Item %d action=%s id=%s failed: %s", i, action, id, reason));
                    }
                }
            }

            if (topLevelErrors && failedItems == 0) {
                addFailureDetail(failureDetails, "Bulk response has errors=true but no failing item details were parsed");
            }

            return new BulkResponseValidation(topLevelErrors, totalItems, failedItems, failureDetails);
        } catch (Exception ex) {
            List<String> details = new ArrayList<String>();
            details.add("Unable to parse bulk response: " + ex.getMessage());
            return new BulkResponseValidation(true, 0, 0, details);
        }
    }

    private static void addFailureDetail(List<String> failureDetails, String message) {
        if (failureDetails.size() < BULK_ERROR_DETAILS_LIMIT) {
            failureDetails.add(message);
        }
    }

    static final class BulkResponseValidation {
        final boolean topLevelErrors;
        final int totalItems;
        final int failedItems;
        final List<String> failureDetails;

        BulkResponseValidation(boolean topLevelErrors, int totalItems, int failedItems, List<String> failureDetails) {
            this.topLevelErrors = topLevelErrors;
            this.totalItems = totalItems;
            this.failedItems = failedItems;
            this.failureDetails = failureDetails;
        }

        boolean hasFailures() {
            return topLevelErrors || failedItems > 0 || !failureDetails.isEmpty();
        }
    }
}
