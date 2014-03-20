package com.netflix.genie.common.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *  Representation of the state of the Cluster  object.
 *  
 * @author amsharma
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
public class ClusterConfigElement implements Serializable {

    private static final long serialVersionUID = 8046582926818942370L;
    
    private static Logger logger = LoggerFactory
            .getLogger(ClusterConfigElement.class);

    /**
     * Unique ID for this cluster.
     */
    @Id
    private String id;

    /**
     * Name for this cluster, e.g. cquery.
     */
    @Basic
    private String name;

    /**
     * User name who created this cluster.
     */
    @Basic
    private String user;

    /**
     * Set of tags for scheduling - e.g. adhoc, sla, vpc, etc.
     */
    @ElementCollection
    private ArrayList<String> tags;

    /**
     * Reference to all the config (xml's) needed for this application.
     */
    @ElementCollection
    private ArrayList<String> configs;
    
    @Transient
    private ArrayList<String> cmds;

    @XmlElement
    public ArrayList<String> getCmds() {
        if(this.commands != null) {
            cmds = new ArrayList<String>();
            Iterator<CommandConfigElement> it = this.commands.iterator();
            while(it.hasNext()){
                cmds.add(((CommandConfigElement)it.next()).getId());
            }
        }
        return cmds;
    }

    public void setCmds(ArrayList<String> cmds) {
        this.cmds = cmds;
    }

    /**
     * Set of commands supported on this cluster - e.g. prodhive, testhive, etc.
     */
    @ManyToMany(targetEntity = CommandConfigElement.class)
    private ArrayList<CommandConfigElement> commands;

    /**
     * Version of this cluster.
     */
    @Basic
    private String version;

    /**
     *  Status of cluster - UP, OUT_OF_SERVICE or TERMINATED.
     */
    @Basic
    private String status;

    /**
     * When was this cluster created?
     */
    @Basic
    private Long createTime;

    /**
     * When was this cluster last updated?
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

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public ArrayList<String> getTags() {
        return tags;
    }

    public void setTags(ArrayList<String> tags) {
        this.tags = tags;
    }

    public ArrayList<String> getConfigs() {
        return configs;
    }

    public void setConfigs(ArrayList<String> configs) {
        this.configs = configs;
    }

    @XmlTransient
    public ArrayList<CommandConfigElement> getCommands() {
        return commands;
    }

    public void setCommands(ArrayList<CommandConfigElement> commands) {
        this.commands = commands;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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