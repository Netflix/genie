package com.netflix.genie.common.model;

import java.io.Serializable;
import java.util.ArrayList;

import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Representation of the state of Application object
 * @author amsharma
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
public class ApplicationElement implements Serializable {

    private static final long serialVersionUID = -3828639247943965669L;

    private static Logger logger = LoggerFactory
            .getLogger(ApplicationElement.class);
    
    /**
     * Unique ID to represent a row in database.
     */
    @Id
    private String id;

    /**
     * Name of this application - e.g. MR, Tez, Jafar.
     */
    @Basic
    private String name;

    /**
     * If it is in use - ACTIVE, DEPRECATED, INACTIVE.
     */
    @Basic
    private String status;

    /**
     * Reference to all the config (xml's) needed for this application.
     */
    private ArrayList<String> configs;

    /**
     * Set of jars required for this application.
     */
    private ArrayList<String> jars;

    /**
     * User who created this application.
     */
    @Basic
    private String user;

    /**
     * Version number for this application.
     */
    @Basic
    private String version;

    /**
     * When was this created?
     */
    @Basic
    private Long createTime;

    /**
     * When was this last updated?
     */
    @Basic
    private Long updateTime;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public ArrayList<String> getConfigs() {
        return configs;
    }

    public void setConfigs(ArrayList<String> configs) {
        this.configs = configs;
    }

    public ArrayList<String> getJars() {
        return jars;
    }

    public void setJars(ArrayList<String> jars) {
        this.jars = jars;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    public Long getUpdateTime() {
        return updateTime;
    }

    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }
}