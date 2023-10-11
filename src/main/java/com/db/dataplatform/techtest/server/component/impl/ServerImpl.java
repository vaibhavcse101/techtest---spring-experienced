package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private final DataBodyService dataBodyServiceImpl;
    private final ModelMapper modelMapper;

    /**
     * @param envelope
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope) {

        // Save to persistence.
        if (Objects.equals(calculateMD5(envelope.getDataBody().getDataBody()), envelope.getDataBody().getCheckSum())) {
            persist(envelope);

            log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());
            return true;
        }
        log.info("Data CheckSum Mismatch, data name: {}", envelope.getDataHeader().getName());
        return false;
    }

    @Override
    public List<DataEnvelope> getDataEnvelopesByBlockType(BlockTypeEnum blockTypeEnum) throws IOException, NoSuchAlgorithmException {
        List<DataBodyEntity> dataBodyEntityList = dataBodyServiceImpl.getDataByBlockType(blockTypeEnum);

        return mapDataBodiesToDataEnvelopes(dataBodyEntityList);
    }

    /**
     * @param blockName
     * @param newBlockType
     * @return
     */
    @Override
    public boolean updateDataEnvelope(String blockName, String newBlockType) {
        Optional<DataBodyEntity> dataBodyOptional = dataBodyServiceImpl.getDataByBlockName(blockName);
        if (dataBodyOptional.isPresent()) {
            DataBodyEntity dataBodyEntity = dataBodyOptional.get();
            DataHeaderEntity dataHeaderEntity = dataBodyEntity.getDataHeaderEntity();
            dataHeaderEntity.setBlocktype(BlockTypeEnum.valueOf(newBlockType));
            dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);
            saveData(dataBodyEntity);
            return true;
        }
        return false;
    }

    public List<DataEnvelope> mapDataBodiesToDataEnvelopes(List<DataBodyEntity> dataBodyEntities) {
        return dataBodyEntities.stream()
                .map(this::mapDataBodyToDataEnvelope)
                .collect(Collectors.toList());
    }

    private DataEnvelope mapDataBodyToDataEnvelope(DataBodyEntity dataBodyEntity) {
        log.info("blockType found :{}", dataBodyEntity.getDataBody());
        DataHeader dataHeader = new DataHeader(dataBodyEntity.getDataHeaderEntity().getName(), dataBodyEntity.getDataHeaderEntity().getBlocktype());
        DataBody dataBody = new DataBody(dataBodyEntity.getDataBody(), dataBodyEntity.getChecksum());
        DataEnvelope dataEnvelope = new DataEnvelope(dataHeader, dataBody);
        log.info("Data Envelope created with Dataheader Name :{},dataBody :{}", dataHeader.getName(), dataBody.getDataBody());
        return dataEnvelope;
    }

    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);
        dataBodyEntity.setChecksum(envelope.getDataBody().getCheckSum());
        saveData(dataBodyEntity);
    }

    private String calculateMD5(String dataBody) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            md5.update(dataBody.getBytes());

            byte[] digest = md5.digest();

            StringBuilder result = new StringBuilder();
            for (byte b : digest) {
                result.append(String.format("%02x", b));
            }

            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }

    private static final String DATA_LAKE_URL = "http://localhost:8090/hadoopserver/pushbigdata";

    @Override
    public CompletableFuture<String> pushDataToDataLakeAsync(String payload) {
        return CompletableFuture.supplyAsync(() -> {
            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                HttpPost httpPost = new HttpPost(DATA_LAKE_URL);
                httpPost.setEntity(new StringEntity(payload));

                // Configure request timeout and connection timeout
                RequestConfig requestConfig = RequestConfig.custom()
                        .setSocketTimeout(60000)  // Socket timeout (adjust as needed)
                        .setConnectTimeout(10000) // Connection timeout (adjust as needed)
                        .build();
                httpPost.setConfig(requestConfig);

                // Execute the request and get the response
                CloseableHttpResponse response = httpClient.execute(httpPost);
                int statusCode = response.getStatusLine().getStatusCode();

                if (statusCode == 200) {
                    HttpEntity responseEntity = response.getEntity();
                    return EntityUtils.toString(responseEntity);
                } else {
                    log.info("Data Lake persistence  Failed");
                }
            } catch (Exception e) {
                // Handle exceptions and retries here
            }
            return null;
        });
    }

}
