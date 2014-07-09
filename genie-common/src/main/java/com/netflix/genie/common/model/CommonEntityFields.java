package com.netflix.genie.common.model;

import java.net.HttpURLConnection;


import javax.persistence.Basic;
import javax.persistence.Column;

import javax.persistence.MappedSuperclass;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.genie.common.exceptions.CloudServiceException;
import com.wordnik.swagger.annotations.ApiModel;
import com.wordnik.swagger.annotations.ApiModelProperty;

/**
 * @author amsharma
 */
@MappedSuperclass
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@ApiModel(value = "Command Fields for all Entities")
public class CommonEntityFields extends Auditable {

    private static final Logger LOG = LoggerFactory.getLogger(CommonEntityFields.class);

    /**
     * Default constructor.
     */
    public CommonEntityFields () {
    }

    /**
     * Version of this entity.
     */
    @Basic(optional = false)
    @Column(name = "version")
    @ApiModelProperty(
            value = "Version number for this entity",
            required = true)
    private String version;

    /**
     * User who created this entity.
     */
    @Basic(optional = false)
    @ApiModelProperty(
            value = "User who created this entity",
            required = true)
    private String user;

    /**
     * Name of this entity
     */
    @Basic(optional = false)
    @ApiModelProperty(
            value = "Name of this entity",
            required = true)
    private String name;

    /**
     * Gets the version of this resource.
     *
     * @return version
     */
    public String getVersion() {
        return this.version;
    }

    /**
     * Sets the version for this resource.
     *
     * @param version version number for this cluster
     */
    public void setVersion(final String version) {
        this.version = version;
    }

    /**
     * Gets the user that created this cluster.
     *
     * @return user
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Sets the user who created this cluster.
     *
     * @param user user who created this cluster. Not null/empty/blank.
     * @throws CloudServiceException
     */
    public void setUser(final String user) throws CloudServiceException {
        if (StringUtils.isBlank(user)) {
            throw new CloudServiceException(
                    HttpURLConnection.HTTP_BAD_REQUEST,
                    "No user Entered.");
        }
        this.user = user;
    }


    /**
     * Gets the name for this entity.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name for this entity.
     *
     * @param name the new name of this entity. Not null/empty/blank
     */
    public void setName(final String name) {
        this.name = name;
    }
}
