package io.github.delirius325.jmeter.backendlistener.elasticsearch;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestElasticSearchMetricSenderBulkResponse {

    @Test
    public void shouldReportSuccessWhenBulkItemsContainNoErrors() {
        String responseBody = "{\"took\":1,\"errors\":false,\"items\":[{\"create\":{\"_index\":\"test\",\"_id\":\"1\",\"status\":201}}]}";

        ElasticSearchMetricSender.BulkResponseValidation validation = ElasticSearchMetricSender.validateBulkResponse(responseBody);

        assertFalse(validation.hasFailures());
        assertEquals(1, validation.totalItems);
        assertEquals(0, validation.failedItems);
    }

    @Test
    public void shouldReportFailureWhenAnyBulkItemContainsError() {
        String responseBody = "{\"took\":2,\"errors\":true,\"items\":["
                + "{\"create\":{\"_index\":\"test\",\"_id\":\"1\",\"status\":201}},"
                + "{\"create\":{\"_index\":\"test\",\"_id\":\"2\",\"status\":400,\"error\":{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed\"}}}"
                + "]}";

        ElasticSearchMetricSender.BulkResponseValidation validation = ElasticSearchMetricSender.validateBulkResponse(responseBody);

        assertTrue(validation.hasFailures());
        assertEquals(2, validation.totalItems);
        assertEquals(1, validation.failedItems);
        assertFalse(validation.failureDetails.isEmpty());
    }

    @Test
    public void shouldReportFailureWhenBulkResponseCannotBeParsed() {
        ElasticSearchMetricSender.BulkResponseValidation validation = ElasticSearchMetricSender.validateBulkResponse("not-json");

        assertTrue(validation.hasFailures());
        assertTrue(validation.topLevelErrors);
        assertFalse(validation.failureDetails.isEmpty());
    }
}

