package org.huifer.kafkawebui.controller;

import org.huifer.kafkawebui.model.ClusterDescription;
import org.huifer.kafkawebui.model.ResultVO;
import org.huifer.kafkawebui.service.kafka.IClusterOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/cluster")
public class ClusterController {
    @Autowired
    private IClusterOperation clusterOperation;

    @GetMapping("/info")
    public ResultVO info() {
        ClusterDescription description = clusterOperation.description();
        return new ResultVO("ok", description, 200);
    }
}
