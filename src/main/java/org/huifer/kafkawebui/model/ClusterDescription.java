package org.huifer.kafkawebui.model;

import java.util.Collection;
import org.apache.kafka.common.Node;

/**
 * kafka 集群信息
 */
public class ClusterDescription {

    private String clusterId;
    private Node controller;
    private Collection<Node> nodes;

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public Node getController() {
        return controller;
    }

    public void setController(Node controller) {
        this.controller = controller;
    }

    public Collection<Node> getNodes() {
        return nodes;
    }

    public void setNodes(Collection<Node> nodes) {
        this.nodes = nodes;
    }
}
