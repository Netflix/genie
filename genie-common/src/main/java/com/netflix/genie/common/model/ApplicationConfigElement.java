package com.netflix.genie.common.model;

import java.io.Serializable;
import java.util.ArrayList;

import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.ElementCollection;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 *  Representation of the state of Application config object
 *  
 * @author amsharma
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
public class ApplicationConfigElement implements Serializable {

    private static final long serialVersionUID = 1L;
    
    /**
     * Unique ID to represent a row in database.
     */
    @Id
    private String id;

    /**
     * Name of this application - e.g. MR1, MR2, Tez etc.
     */ 
    @Basic
    private String name;

    /**
     * If it is in use - ACTIVE, DEPRECATED, INACTIVE.
     */
    @Basic
    private String status;

    /**
     * Reference to all the configs  needed for this application.
     */
    @ElementCollection(fetch=FetchType.EAGER)
    private ArrayList<String> configs;

    /**
     * Set of jars required for this application.
     */
    @ElementCollection(fetch=FetchType.EAGER)
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
    
    /**
     * Users can specify a property file location with environment variables.
     */
    @Basic
    private String envPropFile;

    /**
     * Default constructor.
     */
    public ApplicationConfigElement() {
        
    }

    /**
     * Gets the id (primary key) for this application.
     *
     * @return id
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the id (primary key) for this application.
     *
     * @param id
     *            unique id for this cluster
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the name for this application.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name for this application.
     *
     * @param name
     *            unique id for this cluster
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Gets the status for this application.
     *
     * @return status - possible values: Types.ConfigStatus
     */
    public String getStatus() {
        return status;
    }

    /**
     * Sets the status for this application.
     *
     * @param status
     *            possible values Types.ConfigStatus
     */
    public void setStatus(String status) {
        this.status = status.toUpperCase();
    }
    
    /**
     * Gets the configs for this application.
     *
     * @return configs 
     */
    public ArrayList<String> getConfigs() {
        return configs;
    }

    /**
     * Sets the configs for this application.
     *
     * @param configs
     *            The config files that this application needs
     */
    public void setConfigs(ArrayList<String> configs) {
        this.configs = configs;
    }

    /**
     * Gets the jars for this application.
     *
     * @return jars 
     *              list of jars this application relies on for execution
     */
    public ArrayList<String> getJars() {
        return jars;
    }

    /**
     * Sets the jars needed for this application.
     *
     * @param jars
     *            All jars needed for execution of this application
     */
    public void setJars(ArrayList<String> jars) {
        this.jars = jars;
    }

    /**
     * Gets the user that created this application.
     *
     * @return user
     */
    public String getUser() {
        return user;
    }

    /**
     * Sets the user who created  this application.
     *
     * @param user
     *            user who created this application
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Gets the version of this application.
     *
     * @return version - like 1.2.3
     *          
     */
    public String getVersion() {
        return version;
    }

    /**
     * Sets the version for this application.
     *
     * @param version
     *            version number for this application
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * Gets the create time for this application.
     *
     * @return createTime - epoch time of creation in milliseconds
     * 
     */
    public Long getCreateTime() {
        return createTime;
    }

    /**
     * Sets the create time for this application.
     *
     * @param createTime
     *           epoch time in ms
     */
    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    /**
     * Gets the last updated time for this application.
     *
     * @return updateTime - epoch time of update in milliseconds
     */
    public Long getUpdateTime() {
        return updateTime;
    }

    /**
     * Sets the updated time for this application.
     *
     * @param updateTime
     *            epoch time in milliseconds
     */
    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }
    
    /**
     * Gets the envPropFile name 
     *
     * @return envPropFile - file name containing environment variables.
     */
    public String getEnvPropFile() {
        return envPropFile;
    }

    /**
     * Sets the env property file name in string form.
     *
     * @param envPropFile
     *           contains the list of env variables to set while running a command using this application.
     */
    public void setEnvPropFile(String envPropFile) {
        this.envPropFile = envPropFile;
    }
}
