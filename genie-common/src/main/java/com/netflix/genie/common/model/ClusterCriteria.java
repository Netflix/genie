package com.netflix.genie.common.model;

import java.io.Serializable;
import java.util.ArrayList;

import javax.xml.bind.annotation.XmlElement;

/**
 * @author amsharma
 */
public class ClusterCriteria implements Serializable {

    private static final long serialVersionUID = 1782794735938665541L;

    private ArrayList<String> tagList;
    private String id;
    
    public ClusterCriteria (String id, ArrayList<String> tlist) {
        this.id = id;
        this.tagList = tlist;
    }
    
    public ClusterCriteria() {
        
    }
    
    @XmlElement
    public ArrayList<String> getTagList() {
        return tagList;
    }

    public void setTagList(ArrayList<String> tagList) {
        this.tagList = tagList;
    }

    @XmlElement
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}