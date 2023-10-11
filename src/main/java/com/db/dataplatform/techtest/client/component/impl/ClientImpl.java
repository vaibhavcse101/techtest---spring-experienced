package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataBody;
import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.api.model.DataHeader;
import com.db.dataplatform.techtest.client.component.Client;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ClientImpl implements Client {

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

    private final RestTemplate restTemplate;

    @Override
    public void pushData(DataEnvelope dataEnvelope) {
        log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);
        HttpEntity<DataEnvelope> requestEntity = getDataEnvelopeHttpEntity(dataEnvelope);

        // Send a POST request with the DataEnvelope object as the request body
        ResponseEntity<Boolean> responseEntity = restTemplate.exchange(
                URI_PUSHDATA,
                HttpMethod.POST,
                requestEntity,
                Boolean.class // Change to the expected response type
        );
    }

    private static HttpEntity<DataEnvelope> getDataEnvelopeHttpEntity(DataEnvelope dataEnvelope) {
        HttpHeaders headers = getHttpHeaders();

        // Create an HttpEntity with the DataEnvelope object and headers
        return new HttpEntity<>(dataEnvelope, headers);
    }

    private static HttpHeaders getHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        return headers;
    }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        log.info("Query for data with header block type {}", blockType);
        HttpHeaders headers = getHttpHeaders();

        // Send a GET request to retrieve data
        ResponseEntity<Object[]> responseEntity = restTemplate.exchange(
                URI_GETDATA.expand(blockType), // Replace with the actual blockType
                HttpMethod.GET,
                new HttpEntity<>(headers),
                Object[].class
        );

        Object[] responseObjects = responseEntity.getBody();
        List<DataEnvelope> dataEnvelopes = new ArrayList<>();
        if (responseObjects != null) {
            for (Object responseObject : responseObjects) {
                dataEnvelopes.add(getDataEnvelopeFromString(responseObject.toString()));
            }

            return dataEnvelopes;
        } else {
            return null;
        }
    }

    public DataEnvelope getDataEnvelopeFromString(String jsonString) {
        String dataHeaderString = extractDataHeaderString(jsonString);

        // Parse the "dataBody" object from the string
        String dataBodyString = extractDataBodyString(jsonString);

        // Create a DataHeader object from the parsed dataHeader string
        DataHeader dataHeader = createDataHeaderFromParsedString(dataHeaderString);

        // Create a DataBody object from the parsed dataBody string
        DataBody dataBody = createDataBodyFromParsedString(dataBodyString);

        // Create a DataEnvelope object
        return new DataEnvelope(dataHeader, dataBody);


    }

    // Extract the "dataHeader" object as a string from the input string
    private static String extractDataHeaderString(String jsonString) {
        int startIndex = jsonString.indexOf("dataHeader=");
        int endIndex = findMatchingClosingBraceIndex(jsonString, startIndex);

        if (startIndex != -1 && endIndex != -1) {
            return jsonString.substring(startIndex, endIndex + 1);
        } else {
            throw new IllegalArgumentException("dataHeader not found in the string.");
        }
    }

    // Extract the "dataBody" object as a string from the input string
    private static String extractDataBodyString(String jsonString) {
        int startIndex = jsonString.indexOf("dataBody=");
        int endIndex = findMatchingClosingBraceIndex(jsonString, startIndex);

        if (startIndex != -1 && endIndex != -1) {
            return jsonString.substring(startIndex, endIndex + 1);
        } else {
            throw new IllegalArgumentException("dataBody not found in the string.");
        }
    }

    // Create a DataHeader object from the parsed dataHeader string
    private static DataHeader createDataHeaderFromParsedString(String dataHeaderString) {
        String name = getValueFromParsedString(dataHeaderString, "name");
        String blockType = getValueFromParsedString(dataHeaderString, "blockType");

        return new DataHeader(name, BlockTypeEnum.valueOf(blockType));
    }

    // Create a DataBody object from the parsed dataBody string
    private static DataBody createDataBodyFromParsedString(String dataBodyString) {
        String dataBodyValue = getValueFromParsedString(dataBodyString, "dataBody");

        return new DataBody(dataBodyValue, null);
    }

    private static String getValueFromParsedString(String parsedString, String key) {
        int startIndex = parsedString.indexOf(key + "=");
        int endIndex = parsedString.indexOf(",", startIndex);
        if (endIndex == -1) {
            endIndex = parsedString.indexOf("}", startIndex);
        }
        return parsedString.substring(startIndex + key.length() + 1, endIndex);
    }

    private static int findMatchingClosingBraceIndex(String jsonString, int startIndex) {
        int count = 0;
        for (int i = startIndex; i < jsonString.length(); i++) {
            if (jsonString.charAt(i) == '{') {
                count++;
            } else if (jsonString.charAt(i) == '}') {
                count--;
                if (count == 0) {
                    return i;
                }
            }
        }
        return -1;
    }


    @Override
    public boolean updateData(String blockName, String newBlockType) {
        HttpHeaders headers = getHttpHeaders();

        // Send a PATCH request to update data
        ResponseEntity<Boolean> responseEntity = restTemplate.exchange(
                URI_PATCHDATA.expand(blockName, newBlockType), // Replace with the actual blockName and newBlockType
                HttpMethod.PATCH,
                new HttpEntity<>(headers),
                Boolean.class
        );

        // Handle the response as needed
        if (responseEntity.getStatusCode().is2xxSuccessful()) {
            // Handle the boolean response
            return Boolean.TRUE.equals(responseEntity.getBody());
        } else {
            // Handle the error response
            return false;
        }
    }


}
