package com.netflix.genie.common.model;

import java.io.Serializable;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;

/**
 * @author amsharma
 */
public class ClusterCriteria implements Serializable {

    private static final long serialVersionUID = 1782794735938665541L;

    private ArrayList<String> tags;
    
    public ClusterCriteria (ArrayList<String> tags) {
        this.tags = tags;
    }
    
    public ClusterCriteria() {
        
    }
    
    @XmlElement
    public ArrayList<String> getTags() {
        return tags;
    }

    public void setTags(ArrayList<String> tags) {
        this.tags = tags;
    }
}
