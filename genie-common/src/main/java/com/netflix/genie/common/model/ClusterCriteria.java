package com.netflix.genie.common.model;

import java.io.Serializable;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;

/**
 * @author amsharma
 */
public class ClusterCriteria implements Serializable {

    private static final long serialVersionUID = 1782794735938665541L;

    private ArrayList<String> clusterList;
    private String id;
    
    public ClusterCriteria (String id, ArrayList<String> clist) {
        this.id = id;
        this.clusterList = clist;
    }
    
    public ClusterCriteria() {
        
    }
    
    @XmlElement
    public ArrayList<String> getClusterList() {
        return clusterList;
    }

    public void setClusterList(ArrayList<String> clusterList) {
        this.clusterList = clusterList;
    }

    @XmlElement
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}