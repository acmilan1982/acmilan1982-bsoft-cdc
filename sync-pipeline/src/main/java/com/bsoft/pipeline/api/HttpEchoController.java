package com.bsoft.pipeline.api;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * ClassName: PipelineApplication
 * Package:   com.bsoft.pipeline.api
 * Description:
 *
 * @Author: snoopy
 * @Create: 2026/3/6 - 16:38
 */

@RestController
@RequestMapping("/demo")
public class HttpEchoController {

    @PostMapping(
            value = "/echo",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE
    )
    public JsonNode echo(@RequestBody JsonNode body) {
        return body;
    }
}