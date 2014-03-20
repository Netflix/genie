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
 *  Representation of the state of the Command Object
 *  
 * @author amsharma
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
public class CommandConfigElement implements Serializable {
    
    private static final long serialVersionUID = -6106046473373305992L;
    
    private static Logger logger = LoggerFactory
            .getLogger(CommandConfigElement.class);

    /**
     * Unique ID to represent a row in database.
     */
    @Id
    private String id;

    /**
     * Name of this application - e.g. prodhive, prodpig, etc.
     */
    @Basic
    private String name;

    /**
     * If it is in use - ACTIVE, DEPRECATED, INACTIVE.
     */
    @Basic
    private String status;

    /**
     * Location of the executable for this command on Genie or gateways.
     */
    @Basic
    private String executable;

    /**
     * Reference to all the config (xml's) needed for this command.
     */
    @ElementCollection
    private ArrayList<String> configs;

    @XmlElement
    public ArrayList<String> getAppids() {
        //return appids;
        if(this.applications != null) {
            appids = new ArrayList<String>();
            Iterator<ApplicationConfigElement> it = this.applications.iterator();
            while(it.hasNext()){
                appids.add(((ApplicationConfigElement)it.next()).getId());
            }
        }
        return appids;
    }

    public void setAppids(ArrayList<String> appids) {
        this.appids = appids;
    }

    @Transient
    private ArrayList<String> appids;
    
    /**
     * Set of applications that can run this command - foreign key in database.
     */
    @ManyToMany(targetEntity = ApplicationConfigElement.class)    
    private ArrayList<ApplicationConfigElement> applications;

    /**
     * User who created this command.
     */
    @Basic
    private String user;
    
    /**
     * Jobtype of the command. eg: hive, pig , hadoop etc.
     */
    @Basic
    private String jobType;

    /**
     * Version number for this command.
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

    public String getExecutable() {
        return executable;
    }

    public void setExecutable(String executable) {
        this.executable = executable;
    }

    public ArrayList<String> getConfigs() {
        return configs;
    }

    public void setConfigs(ArrayList<String> configs) {
        this.configs = configs;
    }

    @XmlTransient
    public ArrayList<ApplicationConfigElement> getApplications() {
        return applications;
    }

    public void setApplications(ArrayList<ApplicationConfigElement> applications) {
        this.applications = applications;
    }
    
    public void setApplications() {
        ArrayList<ApplicationConfigElement> appList = new ArrayList<ApplicationConfigElement>();
        Iterator<String> it = this.appids.iterator();
        while(it.hasNext()) {
            ApplicationConfigElement ae = new ApplicationConfigElement();
            ae.setId((String)it.next());
            appList.add(ae);
        }
        this.applications = appList;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
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
