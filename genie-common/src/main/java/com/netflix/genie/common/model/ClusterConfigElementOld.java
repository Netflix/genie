/*
 *
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.genie.common.model;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Cacheable;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Representation of the state of the cluster config object.
 *
 * @author skrishnan
 * @author amsharma
 */
@Entity
@Table(schema = "genie")
@Cacheable(false)
public class ClusterConfigElementOld implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory
            .getLogger(ClusterConfigElementOld.class);

    @Id
    private String id;

    @Basic
    private String name;

    @Basic
    private String user;

    @Basic
    private Boolean prod;

    @Basic
    private Boolean test;

    @Basic
    private Boolean unitTest;

    @Basic
    private Boolean adHoc;

    @Basic
    private Boolean sla;

    @Basic
    private Boolean bonus;

    @Basic
    private String s3CoreSiteXml;

    @Basic
    private String s3MapredSiteXml;

    @Basic
    private String s3HdfsSiteXml;

    @Basic
    private String s3YarnSiteXml;

    @Basic
    private String prodHiveConfigId;

    @Basic
    private String testHiveConfigId;

    @Basic
    private String unitTestHiveConfigId;

    @Basic
    private String prodPigConfigId;

    @Basic
    private String testPigConfigId;

    @Basic
    private String unitTestPigConfigId;

    @Basic
    private String hadoopVersion;

    @Basic
    /* upper case in DB - UP, OUT_OF_SERVICE or TERMINATED */
    private String status;

    @Basic
    private String jobFlowId;

    @Basic
    private Boolean hasStats;

    @Basic
    private Long createTime;

    @Basic
    private Long updateTime;

    /**
     * Default constructor.
     */
    public ClusterConfigElementOld() {
    }

    /**
     * Gets the id (primary key) for this cluster.
     *
     * @return id
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the id (primary key) for this cluster.
     *
     * @param id
     *            unique id for this cluster
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Get the name for this cluster.
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name for this cluster.
     *
     * @param name
     *            non-unique name for this cluster
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Is this a prod cluster?
     *
     * @return Boolean representing whether cluster is prod or not
     */
    public Boolean getProd() {
        return prod;
    }

    /**
     * Set whether the cluster is prod or not.
     *
     * @param prod
     *            prod or not
     */
    public void setProd(Boolean prod) {
        this.prod = prod;
    }

    /**
     * Is this a test cluster?
     *
     * @return Boolean representing whether cluster is test or not
     */
    public Boolean getTest() {
        return test;
    }

    /**
     * Set whether the cluster is test or not.
     *
     * @param test
     *            test or not
     */
    public void setTest(Boolean test) {
        this.test = test;
    }

    /**
     * Is this a unitTest (aka dev) cluster?
     *
     * @return Boolean representing whether the cluster is unitTest or not
     */
    public Boolean getUnitTest() {
        return unitTest;
    }

    /**
     * Set whether the cluster is unitTest (aka dev) or not.
     *
     * @param unitTest
     *            unitTest or not
     */
    public void setUnitTest(Boolean unitTest) {
        this.unitTest = unitTest;
    }

    /**
     * Does this cluster support adHoc jobs?
     *
     * @return Boolean representing whether this is an adhoc cluster or not
     */
    public Boolean getAdHoc() {
        return adHoc;
    }

    /**
     * Set whether the cluster supports adHoc jobs or not.
     *
     * @param doesAdHoc
     *            adHoc or not
     */
    public void setAdHoc(Boolean doesAdHoc) {
        this.adHoc = doesAdHoc;
    }

    /**
     * Does this cluster support sla jobs?
     *
     * @return Boolean representing whether this is an sla cluster or not
     */
    public Boolean getSla() {
        return sla;
    }

    /**
     * Set whether this cluster supports sla jobs or not.
     *
     * @param doesScheduled
     *            sla or not
     */
    public void setSla(Boolean doesScheduled) {
        this.sla = doesScheduled;
    }

    /**
     * Does this cluster support "bonus" jobs?
     *
     * @return Boolean representing whether this is a bonus cluster or not
     */
    public Boolean getBonus() {
        return bonus;
    }

    /**
     * Set whether this cluster supports bonus jobs or not.
     *
     * @param isBonus
     *            bonus or not
     */
    public void setBonus(Boolean isBonus) {
        this.bonus = isBonus;
    }

    /**
     * Get location of core-site.xml for this cluster.
     *
     * @return s3CoreSiteXml s3 (or hdfs) location
     */
    public String getS3CoreSiteXml() {
        return s3CoreSiteXml;
    }

    /**
     * Set location of core-site.xml for this cluster.
     *
     * @param s3CoreSiteXml
     *            s3 (or hdfs) location
     */
    public void setS3CoreSiteXml(String s3CoreSiteXml) {
        this.s3CoreSiteXml = s3CoreSiteXml;
    }

    /**
     * Get location of mapred-site.xml for this cluster.
     *
     * @return s3MapredSiteXml s3 (or hdfs) location
     */
    public String getS3MapredSiteXml() {
        return s3MapredSiteXml;
    }

    /**
     * Set location of mapred-site.xml for this cluster.
     *
     * @param s3MapredSiteXml
     *            s3 (or hdfs) location
     */
    public void setS3MapredSiteXml(String s3MapredSiteXml) {
        this.s3MapredSiteXml = s3MapredSiteXml;
    }

    /**
     * Get location of hdfs-site.xml for this cluster.
     *
     * @return s3HdfsSiteXml s3 (or hdfs) location
     */
    public String getS3HdfsSiteXml() {
        return s3HdfsSiteXml;
    }

    /**
     * Set location of hdfs-site.xml for this cluster.
     *
     * @param s3HdfsSiteXml
     *            s3 (or hdfs) location
     */
    public void setS3HdfsSiteXml(String s3HdfsSiteXml) {
        this.s3HdfsSiteXml = s3HdfsSiteXml;
    }

    /**
     * Get location of yarn-site.xml for this cluster.
     *
     * @return s3YarnSiteXml s3 (or hdfs) location
     */
    public String getS3YarnSiteXml() {
        return s3YarnSiteXml;
    }

    /**
     * Set location of yarn-site.xml for this cluster.
     *
     * @param s3YarnSiteXml
     *            s3 (or hdfs) location
     */
    public void setS3YarnSiteXml(String s3YarnSiteXml) {
        this.s3YarnSiteXml = s3YarnSiteXml;
    }

    /**
     * Get user who last updated this cluster config.
     *
     * @return user
     */
    public String getUser() {
        return user;
    }

    /**
     * Set user name to update this cluster config.
     *
     * @param user
     *            user name
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Get the hiveConfigID for prod hive on this cluster.
     *
     * @return prodHiveConfigId unique id for hive config
     */
    public String getProdHiveConfigId() {
        return prodHiveConfigId;
    }

    /**
     * Set the hiveConfigId for prod hive on this cluster.
     *
     * @param prodHiveConfigId
     *            unique id for hive config
     */
    public void setProdHiveConfigId(String prodHiveConfigId) {
        this.prodHiveConfigId = prodHiveConfigId;
    }

    /**
     * Get the hiveConfigID for test hive on this cluster.
     *
     * @return testHiveConfigId unique id for hive config
     */
    public String getTestHiveConfigId() {
        return testHiveConfigId;
    }

    /**
     * Set the hiveConfigId for test hive on this cluster.
     *
     * @param testHiveConfigId
     *            unique id for hive config
     */
    public void setTestHiveConfigId(String testHiveConfigId) {
        this.testHiveConfigId = testHiveConfigId;
    }

    /**
     * Get the hiveConfigId for unitTest (aka dev) on this cluster.
     *
     * @return unitTestHiveConfigId unique id for hive config
     */
    public String getUnitTestHiveConfigId() {
        return unitTestHiveConfigId;
    }

    /**
     * Set the hiveConfigId for unitTest (aka dev) on this cluster.
     *
     * @param unitTestHiveConfigId
     *            unique id for hive config
     */
    public void setUnitTestHiveConfigId(String unitTestHiveConfigId) {
        this.unitTestHiveConfigId = unitTestHiveConfigId;
    }

    /**
     * Get the pigConfigId for prod pig on this cluster.
     *
     * @return prodPigConfigId unique id for pig config
     */
    public String getProdPigConfigId() {
        return prodPigConfigId;
    }

    /**
     * Set the pigConfigId for prod pig on this cluster.
     *
     * @param prodPigConfigId
     *            unique id for pig config
     */
    public void setProdPigConfigId(String prodPigConfigId) {
        this.prodPigConfigId = prodPigConfigId;
    }

    /**
     * Get the pigConfigId for test pig on this cluster.
     *
     * @return testPigConfigId unique id for pig config
     */
    public String getTestPigConfigId() {
        return testPigConfigId;
    }

    /**
     * Set the pigConfigId for test pig on this cluster.
     *
     * @param testPigConfigId
     *            unique id for pig config
     */
    public void setTestPigConfigId(String testPigConfigId) {
        this.testPigConfigId = testPigConfigId;
    }

    /**
     * Get the pigConfigId for unitTest (aka dev) pig on this cluster.
     *
     * @return unitTestPigConfigId unique id for pig config
     */
    public String getUnitTestPigConfigId() {
        return unitTestPigConfigId;
    }

    /**
     * Set the pigConfigId for unitTest (aka dev) pig on this cluster.
     *
     * @param unitTestPigConfigId
     *            unique id for pig config
     */
    public void setUnitTestPigConfigId(String unitTestPigConfigId) {
        this.unitTestPigConfigId = unitTestPigConfigId;
    }

    /**
     * Get Hadoop version on this cluster.
     *
     * @return hadoopVersion
     */
    public String getHadoopVersion() {
        return hadoopVersion;
    }

    /**
     * Set Hadoop version for this cluster.
     *
     * @param hadoopVersion
     *            version of hadoop (e.g. 1.0.3)
     */
    public void setHadoopVersion(String hadoopVersion) {
        this.hadoopVersion = hadoopVersion;
    }

    /**
     * Get status of this cluster.
     *
     * @return status - possible values: Types.ClusterStatus
     */
    public String getStatus() {
        return status;
    }

    /**
     * Set status for this cluster.
     *
     * @param status
     *            possible values: Types.ClusterStatus
     */
    public void setStatus(String status) {
        this.status = status.toUpperCase();
    }

    /**
     * Gets the jobFlowId if it is using EMR.
     *
     * @return jobFlowId
     */
    public String getJobFlowId() {
        return jobFlowId;
    }

    /**
     * Set jobFlowId for EMR clusters.
     *
     * @param jobFlowId
     *            EMR jobflow ID
     */
    public void setJobFlowId(String jobFlowId) {
        this.jobFlowId = jobFlowId;
    }

    /**
     * Are statistics being collected for this cluster (if it is EMR).
     *
     * @return hasStats
     */
    public Boolean getHasStats() {
        return hasStats;
    }

    /**
     * Set if statistics are being collected for this cluster (if it is EMR).
     *
     * @param hasStats
     *            has stats or not
     */
    public void setHasStats(Boolean hasStats) {
        this.hasStats = hasStats;
    }

    /**
     * Get the create time for this cluster.
     *
     * @return createTime in ms
     */
    public Long getCreateTime() {
        return createTime;
    }

    /**
     * Set the create time for this cluster.
     *
     * @param createTime
     *            epoch time in ms
     */
    public void setCreateTime(Long createTime) {
        this.createTime = createTime;
    }

    /**
     * Get the last update time for this cluster.
     *
     * @return updateTime in ms
     */
    public Long getUpdateTime() {
        return updateTime;
    }

    /**
     * Set the last update time for this cluster.
     *
     * @param updateTime
     *            epoch time in ms
     */
    public void setUpdateTime(Long updateTime) {
        this.updateTime = updateTime;
    }

    /**
     * Get the hiveConfigId for this cluster by configType.
     *
     * @param configType
     *            type of hiveConfig to return for this cluster
     * @return appropriate hiveConfigId
     */
    public String getHiveConfigId(Types.Configuration configType) {
        LOG.debug("called");
        if (configType == null) {
            return null;
        } else if (configType == Types.Configuration.PROD) {
            return prodHiveConfigId;
        } else if (configType == Types.Configuration.TEST) {
            return testHiveConfigId;
        } else {
            return unitTestHiveConfigId;
        }
    }

    /**
     * Get the pigConfigId for this cluster by configType.
     *
     * @param configType
     *            type of pigConfig to return for this cluster
     * @return appropriate pigConfigId
     */
    public String getPigConfigId(Types.Configuration configType) {
        LOG.debug("called");
        if (configType == null) {
            return null;
        } else if (configType == Types.Configuration.PROD) {
            return prodPigConfigId;
        } else if (configType == Types.Configuration.TEST) {
            return testPigConfigId;
        } else {
            return unitTestPigConfigId;
        }
    }

    /**
     * Return the *-site.xml's as CSVs.
     *
     * @return (s3CoreSiteXml, s3MapredSiteXml, s3HdfsSiteXml)
     */
    public String getS3SiteXmlsAsCsv() {
        LOG.debug("called");

        StringBuilder csv = new StringBuilder();
        if (s3CoreSiteXml != null) {
            csv.append(s3CoreSiteXml);
        }

        if (s3MapredSiteXml != null) {
            csv.append(",");
            csv.append(s3MapredSiteXml);
        }

        if (s3HdfsSiteXml != null) {
            csv.append(",");
            csv.append(s3HdfsSiteXml);
        }

        if (s3YarnSiteXml != null) {
            csv.append(",");
            csv.append(s3YarnSiteXml);
        }

        return csv.toString();
    }

}
