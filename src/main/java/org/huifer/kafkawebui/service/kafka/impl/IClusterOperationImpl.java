package org.huifer.kafkawebui.service.kafka.impl;

import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.huifer.kafkawebui.model.ClusterDescription;
import org.huifer.kafkawebui.service.kafka.IClusterOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.concurrent.ExecutionException;

@Service
public class IClusterOperationImpl extends AbsKafkaOperation implements IClusterOperation {
    private static final Logger LOG = LoggerFactory.getLogger(IClusterOperationImpl.class);

    @Override
    public ClusterDescription description() {
        DescribeClusterResult describeClusterResult = adminClient.describeCluster();
        try {
            Collection<Node> nodes = describeClusterResult.nodes().get();
            KafkaFuture<Node> controller = describeClusterResult.controller();
            String clusterId = describeClusterResult.clusterId().get();

            ClusterDescription clusterDescription = new ClusterDescription();
            clusterDescription.setClusterId(clusterId);
            clusterDescription.setController(controller.get());
            clusterDescription.setNodes(nodes);
            return clusterDescription;
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("获取卡夫卡集群失败,{}", e);

        }
        return null;
    }
}
