package com.db.dataplatform.techtest.server.api.controller;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.ConstraintViolationException;
import javax.validation.Valid;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Controller
@RequestMapping("/dataserver")
@RequiredArgsConstructor
@Validated
public class ServerController {

    private final Server server;

    @PostMapping(value = "/pushdata", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> pushData(@Valid @RequestBody DataEnvelope dataEnvelope) throws IOException, NoSuchAlgorithmException {

        log.info("Data envelope received: {}", dataEnvelope.getDataHeader().getName());
        boolean checksumPass = server.saveDataEnvelope(dataEnvelope);
        if(!checksumPass)
            throw new ConstraintViolationException("Constraint Violation",null);
        CompletableFuture<String> future = server.pushDataToDataLakeAsync(dataEnvelope.getDataBody().getDataBody());
        if(future!=null){
        future.thenAccept(response -> {
            if (response != null) {
                System.out.println("Data push was successful. Response: " + response);
            } else {
                System.out.println("Data push failed or encountered an error.");
            }
        });}
        log.info("Data envelope persisted. Attribute name: {}", dataEnvelope.getDataHeader().getName());
        return ResponseEntity.ok(checksumPass);
    }

    @GetMapping(value = "/data/{blockType}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<DataEnvelope>> getData(@PathVariable BlockTypeEnum blockType) throws IOException, NoSuchAlgorithmException {

        log.info(" BlockType received: {}", blockType);
        //boolean checksumPass = server.saveDataEnvelope(dataEnvelope);

        List<DataEnvelope> dataEnvelopes = server.getDataEnvelopesByBlockType(blockType);
        log.info("Number of data envelopes which matched block type:{} are: {}", blockType, dataEnvelopes.size());
        return ResponseEntity.ok(dataEnvelopes);
    }

    @PatchMapping(value = "/update/{name}/{newBlockType}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> updateData(@PathVariable String name, @PathVariable String newBlockType) {

        log.info(" newBlockType and name received: {} :{}", newBlockType, name);

        Boolean updateDone = server.updateDataEnvelope(name, newBlockType);
        log.info("updateDone for block Name :{} , is : {}", name, updateDone);
        return ResponseEntity.ok(updateDone);
    }


}
