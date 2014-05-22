package com.netflix.genie.common.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;

/**
 * Cluster Criteria.
 *
 * @author amsharma
 */
public class ClusterCriteria implements Serializable {

    private static final long serialVersionUID = 1782794735938665541L;

    //TODO: Switch to List<>
    private final ArrayList<String> tags = new ArrayList<String>();

    /**
     * Create a cluster criteria object with the included tags.
     * @param tags The tags to add
     */
    public ClusterCriteria(final List<String> tags) {
        this.tags.addAll(tags);
    }

    /**
     * Default constructor.
     */
    public ClusterCriteria() {
    }

    /**
     * Get the tags for this cluster criteria.
     * @return The tags for this criteria
     */
    @XmlElement
    public ArrayList<String> getTags() {
        //TODO: Switch to unmodifiable list
        return this.tags;
    }

    /**
     * Set the tags for the cluster criteria.
     * @param tags The tags to set
     */
    public void setTags(final List<String> tags) {
        this.tags.clear();
        this.tags.addAll(tags);
    }
}
