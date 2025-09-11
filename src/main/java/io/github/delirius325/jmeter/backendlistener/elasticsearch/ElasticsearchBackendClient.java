package io.github.delirius325.jmeter.backendlistener.elasticsearch;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.http.AWSRequestSigningApacheInterceptor;
import com.google.gson.Gson;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.TrustStrategy;
import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.util.JMeterUtils;
import org.apache.jmeter.visualizers.backend.AbstractBackendListenerClient;
import org.apache.jmeter.visualizers.backend.BackendListenerContext;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ElasticsearchBackendClient extends AbstractBackendListenerClient {

    private static final String BUILD_NUMBER = "BuildNumber";
    private static final String ES_SCHEME = "es.scheme";
    private static final String ES_HOST = "es.host";
    private static final String ES_PORT = "es.port";
    private static final String ES_INDEX = "es.index";
    private static final String ES_FIELDS = "es.fields";
    private static final String ES_TIMESTAMP = "es.timestamp";
    private static final String ES_BULK_SIZE = "es.bulk.size";
    private static final String ES_TIMEOUT_MS = "es.timout.ms";
    private static final String ES_SAMPLE_FILTER = "es.sample.filter";
    private static final String ES_TEST_MODE = "es.test.mode";
    private static final String ES_AUTH_USER = "es.xpack.user";
    private static final String ES_AUTH_PWD = "es.xpack.password";
    private static final String ES_PARSE_REQ_HEADERS = "es.parse.all.req.headers";
    private static final String ES_PARSE_RES_HEADERS = "es.parse.all.res.headers";
    private static final String ES_AWS_ENDPOINT = "es.aws.endpoint";
    private static final String ES_AWS_REGION = "es.aws.region";
    private static final String ES_SSL_TRUSTSTORE_PATH = "es.ssl.truststore.path";
    private static final String ES_SSL_TRUSTSTORE_PW = "es.ssl.truststore.pw";
    private static final String ES_SSL_KEYSTORE_PATH = "es.ssl.keystore.path";
    private static final String ES_SSL_KEYSTORE_PW = "es.ssl.keystore.pw";
    private static final String ES_SSL_VERIFICATION_MODE = "es.ssl.verificationMode";
    private static final long DEFAULT_TIMEOUT_MS = 200L;
    private static final String SERVICE_NAME = "es";
    private static RestClient client;
    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchBackendClient.class);
    private static final AWSCredentialsProvider credentialsProvider = new DefaultAWSCredentialsProviderChain();
    private static final Map<String, String> DEFAULT_ARGS = new LinkedHashMap<>();

    static {
        DEFAULT_ARGS.put(ES_SCHEME, "http");
        DEFAULT_ARGS.put(ES_HOST, null);
        DEFAULT_ARGS.put(ES_PORT, "9200");
        DEFAULT_ARGS.put(ES_INDEX, null);
        DEFAULT_ARGS.put(ES_TIMESTAMP, "yyyy-MM-dd'T'HH:mm:ss.SSSZZ");
        DEFAULT_ARGS.put(ES_BULK_SIZE, "100");
        DEFAULT_ARGS.put(ES_TIMEOUT_MS, Long.toString(DEFAULT_TIMEOUT_MS));
        DEFAULT_ARGS.put(ES_SAMPLE_FILTER, null);
        DEFAULT_ARGS.put(ES_FIELDS, null);
        DEFAULT_ARGS.put(ES_TEST_MODE, "info");
        DEFAULT_ARGS.put(ES_AUTH_USER, "");
        DEFAULT_ARGS.put(ES_AUTH_PWD, "");
        DEFAULT_ARGS.put(ES_PARSE_REQ_HEADERS, "false");
        DEFAULT_ARGS.put(ES_PARSE_RES_HEADERS, "false");
        DEFAULT_ARGS.put(ES_AWS_ENDPOINT, "");
        DEFAULT_ARGS.put(ES_AWS_REGION, "");
        DEFAULT_ARGS.put(ES_SSL_TRUSTSTORE_PATH, "");
        DEFAULT_ARGS.put(ES_SSL_TRUSTSTORE_PW, "");
        DEFAULT_ARGS.put(ES_SSL_KEYSTORE_PATH, "");
        DEFAULT_ARGS.put(ES_SSL_KEYSTORE_PW, "");
        DEFAULT_ARGS.put(ES_SSL_VERIFICATION_MODE, "full");
    }

    private ElasticSearchMetricSender sender;
    private Set<String> modes;
    private Set<String> filters;
    private Set<String> fields;
    private int buildNumber;
    private int bulkSize;
    private int esVersion;
    private long timeoutMs;

    private static final TrustStrategy TRUST_ALL_STRATEGY = (chain, authType) -> true;

    @Override
    public Arguments getDefaultParameters() {
        Arguments arguments = new Arguments();
        DEFAULT_ARGS.forEach(arguments::addArgument);
        return arguments;
    }

    @Override
    public void setupTest(BackendListenerContext context) throws Exception {
        // Log4J DEBUG log level is recognized by JMeter for this class i.g.d.j.b.e.ElasticsearchBackendClient
        // jmeter_command+=" -L 'io.github.delirius325.jmeter.backendlistener=DEBUG'" but not INFO log level ?!?!
        logger.debug("Execute setupTest using Elasticsearch at {}:///{}:{}", context.getParameter(ES_SCHEME), context.getParameter(ES_HOST), context.getParameter(ES_PORT));
        try {
            this.filters = new HashSet<>();
            this.fields = new HashSet<>();
            this.modes = new HashSet<>(Arrays.asList("info", "debug", "error", "quiet"));
            this.bulkSize = Integer.parseInt(context.getParameter(ES_BULK_SIZE));
            this.timeoutMs = Integer.parseInt((context.getParameter(ES_TIMEOUT_MS)));
            this.buildNumber = (JMeterUtils.getProperty(ElasticsearchBackendClient.BUILD_NUMBER) != null
                    && !JMeterUtils.getProperty(ElasticsearchBackendClient.BUILD_NUMBER).trim().equals(""))
                    ? Integer.parseInt(JMeterUtils.getProperty(ElasticsearchBackendClient.BUILD_NUMBER)) : 0;

            setSSLConfiguration(context);

            if (context.getParameter(ES_AWS_ENDPOINT).equalsIgnoreCase("")) {
                client = RestClient
                        .builder(new HttpHost(context.getParameter(ES_HOST),
                                Integer.parseInt(context.getParameter(ES_PORT)), context.getParameter(ES_SCHEME)))
                        .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
                            if (context.getParameter(ES_SSL_VERIFICATION_MODE).equalsIgnoreCase("none")) {
                                logger.debug("Will trust all remote SSL certificates.");
                                final SSLContextBuilder contextBuilder = new SSLContextBuilder();
                                try {
                                    contextBuilder.loadTrustMaterial(TRUST_ALL_STRATEGY);
                                    httpAsyncClientBuilder.setSSLContext(contextBuilder.build());
                                    httpAsyncClientBuilder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                                } catch (Exception e) {
                                    // NOTE: purposedly ignored as this strategy does not use any custom algorithm
                                    // or certificate
                                }
                            }
                            return httpAsyncClientBuilder;
                        })
                        .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(5000)
                                .setSocketTimeout((int) timeoutMs))
                        .setFailureListener(new RestClient.FailureListener() {
                            @Override
                            public void onFailure(Node node) {
                                logger.error("Error with node: " + node.toString());
                            }
                        }).build();
            } else {
                AWS4Signer signer = new AWS4Signer();
                signer.setServiceName(SERVICE_NAME);
                signer.setRegionName(context.getParameter(ES_AWS_REGION));
                HttpRequestInterceptor interceptor = new AWSRequestSigningApacheInterceptor(SERVICE_NAME, signer,
                        credentialsProvider);
                client = RestClient.builder(HttpHost.create(context.getParameter(ES_AWS_ENDPOINT)))
                        .setHttpClientConfigCallback(hacb -> hacb.addInterceptorLast(interceptor)).build();
            }

            convertParameterToSet(context, ES_SAMPLE_FILTER, this.filters);
            convertParameterToSet(context, ES_FIELDS, this.fields);

            this.sender = new ElasticSearchMetricSender(client, context.getParameter(ES_INDEX).toLowerCase(),
                    context.getParameter(ES_AUTH_USER), context.getParameter(ES_AUTH_PWD),
                    context.getParameter(ES_AWS_ENDPOINT));
            this.sender.createIndex();
            logger.debug("Checking the Elasticsearch version");
            this.esVersion = sender.getElasticSearchVersion();

            checkTestMode(context.getParameter(ES_TEST_MODE));
            super.setupTest(context);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to connect to the ElasticSearch engine", e);
        }
    }

    /**
     * Method that converts a semicolon separated list contained in a parameter into a string set
     *
     * @param context
     * @param parameter
     * @param set
     */
    private void convertParameterToSet(BackendListenerContext context, String parameter, Set<String> set) {
        String value = context.getParameter(parameter);

        convertValueToSet(parameter, set, value);
    }

    private static void convertValueToSet(String parameter, Set<String> set, String value) {
        String[] array = (value.contains(";")) ? value.split(";")
                : new String[]{value};
        if (array.length > 0 && !array[0].trim().equals("")) {
            for (String entry : array) {
                set.add(entry.toLowerCase().trim());
                if (logger.isDebugEnabled())
                    logger.debug("Parsed from " + parameter + ": " + entry.toLowerCase().trim());
            }
        }
    }

    /**
     * Method that sets the SSL configuration to be able to send requests to a secured endpoint
     *
     * @param context
     */
    private void setSSLConfiguration(BackendListenerContext context) {
        String keyStorePath = context.getParameter(ES_SSL_KEYSTORE_PATH);
        if (!keyStorePath.equalsIgnoreCase("")) {
            logger.warn("KeyStore system properties overwritten by ES SSL configuration.");
            System.setProperty("javax.net.ssl.keyStore", keyStorePath);
            System.setProperty("javax.net.ssl.keyStorePassword", context.getParameter(ES_SSL_KEYSTORE_PW));
            switch (FilenameUtils.getExtension(keyStorePath)) {
                case "jks":
                    System.setProperty("javax.net.ssl.keyStoreType", "jks");
                    break;
                case "p12":
                    System.setProperty("javax.net.ssl.keyStoreType", "pkcs12");
                    break;
                default:
                    System.setProperty("javax.net.ssl.keyStoreType", "");
                    break;
            }
        }

        String trustStorePath = context.getParameter(ES_SSL_TRUSTSTORE_PATH);
        if (!trustStorePath.equalsIgnoreCase("")) {
            logger.warn("TrustStore system properties overwritten by ES SSL configuration.");
            System.setProperty("javax.net.ssl.trustStore", trustStorePath);
            System.setProperty("javax.net.ssl.trustStorePassword", context.getParameter(ES_SSL_TRUSTSTORE_PW));
            switch (FilenameUtils.getExtension(trustStorePath)) {
                case "jks":
                    System.setProperty("javax.net.ssl.trustStoreType", "jks");
                    break;
                case "p12":
                    System.setProperty("javax.net.ssl.trustStoreType", "pkcs12");
                    break;
                default:
                    System.setProperty("javax.net.ssl.trustStoreType", "");
                    break;
            }
        }

    }

    @Override
    public void handleSampleResults(List<SampleResult> results, BackendListenerContext context) {
        if (results.isEmpty()) {
            logger.warn("There are no sampler results to handle!");
            return;
        }

        List<String> paramNames = new ArrayList<>();
        context.getParameterNamesIterator().forEachRemaining(paramNames::add);
        // paramNames.stream().sorted().forEach(name -> logger.debug("Parameter: {} = {}", name, context.getParameter(name)));

        final String filtersAsString = (this.filters.isEmpty()) ? "no filters" : String.join(", ", this.filters);
        for (SampleResult sr : results) {
            ElasticSearchMetric metric = new ElasticSearchMetric(sr, context.getParameter(ES_TEST_MODE),
                    context.getParameter(ES_TIMESTAMP), this.buildNumber,
                    context.getBooleanParameter(ES_PARSE_REQ_HEADERS, false),
                    context.getBooleanParameter(ES_PARSE_RES_HEADERS, false), fields);

            if (validateSample(context, sr)) {
                try {
                    this.sender.addToList(new Gson().toJson(metric.getMetric(context)));
                } catch (Exception e) {
                    logger.error(
                            "The ElasticSearch Backend Listener was unable to add sampler to the list of samplers to send... More info in JMeter's console.");
                    e.printStackTrace();
                }
                logger.debug("Sampler result processed {} with filters {} (ES_SAMPLE_FILTER {})", sr.getSampleLabel(), filtersAsString, context.getParameter(ES_SAMPLE_FILTER));
            } else {
                logger.debug("Sampler result skipped {} with filters {} (ES_SAMPLE_FILTER {})", sr.getSampleLabel(), filtersAsString, context.getParameter(ES_SAMPLE_FILTER));
            }
        }

        if (this.sender.getListSize() >= this.bulkSize) {
            try {
                this.sender.sendRequest(this.esVersion);
            } catch (Exception e) {
                logger.error("Error occurred while sending bulk request.", e);
            } finally {
                this.sender.clearList();
            }
        }
    }

    @Override
    public void teardownTest(BackendListenerContext context) throws Exception {
        if (this.sender.getListSize() > 0) {
            this.sender.sendRequest(this.esVersion);
        }
        this.sender.closeConnection();
        super.teardownTest(context);
    }

    /**
     * This method checks if the test mode is valid
     *
     * @param mode The test mode as String
     */
    private void checkTestMode(String mode) {
        if (!this.modes.contains(mode)) {
            logger.warn(
                    "The parameter \"es.test.mode\" isn't set properly. Three modes are allowed: debug ,info, and quiet.");
            logger.warn(
                    " -- \"debug\": sends request and response details to ElasticSearch. Info only sends the details if the response has an error.");
            logger.warn(" -- \"info\": should be used in production");
            logger.warn(" -- \"error\": should be used if you.");
            logger.warn(" -- \"quiet\": should be used if you don't care to have the details.");
        }
    }

    /**
     * This method will validate the current sample to see if it is part of the filters or not.
     *
     * @param context The Backend Listener's context
     * @param sr      The current SampleResult
     * @return true or false depending on whether the sample is valid
     */
    private boolean validateSample(BackendListenerContext context, SampleResult sr) {
        boolean valid = true;
        String sampleLabel = sr.getSampleLabel().toLowerCase().trim();

        if (!this.filters.isEmpty()) {
            for (String filter : filters) {
                Pattern pattern = Pattern.compile(String.format(".*%s.*", filter));
                Matcher matcher = pattern.matcher(sampleLabel);

                if (matcher.find()) {
                    // README.md#Features*Filters
                    // You can also choose to exclude certain samplers; `!!exclude_this;filter1;filter2`
                    valid = !sampleLabel.startsWith("!!");
                    break;
                } else {
                    logger.warn(String.format("Filter out sample label %s, it may start with '!!' or it may not match the filter %s. " +
                            "Check the %s property value in the JMX file. Leave it empty to increase validation", sampleLabel, filter, ES_SAMPLE_FILTER));
                    valid = false;
                }
            }
        }

        // if sample is successful but test mode is "error" only
        if (sr.isSuccessful() && context.getParameter(ES_TEST_MODE).trim().equalsIgnoreCase("error") && valid) {
            valid = false;
        }

        return valid;
    }
}
