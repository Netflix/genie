package com.netflix.genie.web.controllers;

import com.netflix.genie.common.dto.Application;
import com.netflix.genie.common.dto.ApplicationStatus;
import com.netflix.genie.common.dto.Cluster;
import com.netflix.genie.common.dto.ClusterCriteria;
import com.netflix.genie.common.dto.ClusterStatus;
import com.netflix.genie.common.dto.Command;
import com.netflix.genie.common.dto.CommandStatus;
import com.netflix.genie.common.dto.JobRequest;
import com.netflix.genie.core.jpa.repositories.JpaApplicationRepository;
import com.netflix.genie.core.jpa.repositories.JpaClusterRepository;
import com.netflix.genie.core.jpa.repositories.JpaCommandRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobExecutionRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRepository;
import com.netflix.genie.core.jpa.repositories.JpaJobRequestRepository;
import lombok.extern.slf4j.Slf4j;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Integration tests for Jobs REST API.
 *
 * @author amsharma
 * @since 3.0.0
 *
 */
@Slf4j
public class JobRestControllerIntegrationTests extends RestControllerIntegrationTestsBase {

    private static final String BASE_DIR
        = "com/netflix/genie/web/controllers/JobRestControllerIntegrationTests/";
    private static final String FILE_DELIMITER = "/";

    private static final String JOB_ID = "123";
    private static final String JOB_NAME = "List Directories bash job";
    private static final String JOB_USER = "genie";
    private static final String JOB_VERSION = "1.0";

    private static final String APP1_ID = "app1";
    private static final String APP1_NAME = "Application 1";
    private static final String APP1_USER = "genie";
    private static final String APP1_VERSION = "1.0";

    private static final String APP2_ID = "app2";
    private static final String APP2_NAME = "Application 2";
    private static final String APP2_USER = "genie";
    private static final String APP2_VERSION = "1.0";

    private static final String CMD1_ID = "cmd1";
    private static final String CMD1_NAME = "Unix Bash command";
    private static final String CMD1_USER = "genie";
    private static final String CMD1_VERSION = "1.0";

    private static final String CLUSTER1_ID = "cluster1";
    private static final String CLUSTER1_NAME = "Local laptop";
    private static final String CLUSTER1_USER = "genie";
    private static final String CLUSTER1_VERSION = "1.0";

    // Since we're bringing the service up on random port need to figure out what it is
    @Value("${local.server.port}")
    private int port;
    private String appsBaseUrl;
    private String jobsBaseUrl;
    private ResourceLoader resourceLoader;

    @Autowired
    private JpaJobRepository jobRepository;

    @Autowired
    private JpaJobRequestRepository jobRequestRepository;

    @Autowired
    private JpaJobExecutionRepository jobExecutionRepository;

    @Autowired
    private JpaApplicationRepository applicationRepository;

    @Autowired
    private JpaCommandRepository commandRepository;

    @Autowired
    private JpaClusterRepository clusterRepository;

    /**
     * Setup for tests.
     * @throws Exception If there is an error.
     */
    @Before
    public void setup() throws Exception {
        super.setup();
        this.resourceLoader = new DefaultResourceLoader();
        //this.jobsBaseUrl = "http://localhost:" + this.port + "/api/v3/jobs";
        createAnApplication(this.APP1_ID, this.APP1_NAME);
        createAnApplication(this.APP2_ID, this.APP2_NAME);
        createAllClusters();
        createAllCommands();
        linkAllEntities();
    }

    private void linkAllEntities() throws Exception {

        final List<String> apps = new ArrayList<>();
        apps.add(APP1_ID);
        apps.add(APP2_ID);

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(COMMANDS_API + FILE_DELIMITER + CMD1_ID + FILE_DELIMITER + APPLICATIONS_LINK_KEY)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(apps))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());

        final List<String> cmds = new ArrayList<>();
        cmds.add(CMD1_ID);

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(CLUSTERS_API + FILE_DELIMITER + CLUSTER1_ID + FILE_DELIMITER + COMMANDS_LINK_KEY)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(cmds))
            )
            .andExpect(MockMvcResultMatchers.status().isNoContent());
    }

    private void createAnApplication(
        final String id,
        final String app1Name
    ) throws Exception {

        final String setUpFile = this.resourceLoader.getResource(
            this.BASE_DIR
                + id
                + FILE_DELIMITER
                + "setupfile"
        ).getFile().getAbsolutePath();

        final Set<String> app1Dependencies = new HashSet<>();
        final String depFile1 = this.resourceLoader.getResource(
            this.BASE_DIR
                + id
                + FILE_DELIMITER
                + "dep1"
        ).getFile().getAbsolutePath();
        final String depFile2 = this.resourceLoader.getResource(
            this.BASE_DIR
                + id
                + FILE_DELIMITER
                + "dep2"
        ).getFile().getAbsolutePath();
        app1Dependencies.add(depFile1);
        app1Dependencies.add(depFile2);

        final Set<String> app1Configs = new HashSet<>();
        final String configFile1 = this.resourceLoader.getResource(
            this.BASE_DIR
                + id
                + FILE_DELIMITER
                + "config1"
        ).getFile().getAbsolutePath();
        final String configFile2 = this.resourceLoader.getResource(
            this.BASE_DIR
                + id
                + FILE_DELIMITER
                + "config2"
        ).getFile().getAbsolutePath();
        app1Configs.add(configFile1);
        app1Configs.add(configFile2);

        final Application app = new Application.Builder(
            app1Name,
            APP1_USER,
            APP1_VERSION,
            ApplicationStatus.ACTIVE)
            .withId(id)
            .withSetupFile(setUpFile)
            .withConfigs(app1Configs)
            .withDependencies(app1Dependencies)
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(APPLICATIONS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(app))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();
    }

    private void createAllClusters() throws Exception {

        final String setUpFile = this.resourceLoader.getResource(
            BASE_DIR
                + CLUSTER1_ID
                + FILE_DELIMITER
                + "setupfile"
        ).getFile().getAbsolutePath();

        final Set<String> configs = new HashSet<>();
        final String configFile1 = this.resourceLoader.getResource(
            BASE_DIR
                + CLUSTER1_ID
                + FILE_DELIMITER
                + "config1"
        ).getFile().getAbsolutePath();
        final String configFile2 = this.resourceLoader.getResource(
            BASE_DIR
                + CLUSTER1_ID
                + FILE_DELIMITER
                + "config2"
        ).getFile().getAbsolutePath();
        configs.add(configFile1);
        configs.add(configFile2);

        final Set<String> tags = new HashSet<>();
        tags.add("localhost");

        final Cluster cluster = new Cluster.Builder(
            CLUSTER1_NAME,
            CLUSTER1_USER,
            CLUSTER1_VERSION,
            ClusterStatus.UP
        )
            .withId(CLUSTER1_ID)
            .withSetupFile(setUpFile)
            .withConfigs(configs)
            .withTags(tags)
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(CLUSTERS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(cluster))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();
    }

    private void createAllCommands() throws Exception {

        final String setUpFile = this.resourceLoader.getResource(
                BASE_DIR
                + CMD1_ID
                + FILE_DELIMITER
                + "setupfile"
        ).getFile().getAbsolutePath();

        final Set<String> configs = new HashSet<>();
        final String configFile1 = this.resourceLoader.getResource(
                BASE_DIR
                + CMD1_ID
                + FILE_DELIMITER
                + "config1"
        ).getFile().getAbsolutePath();
        final String configFile2 = this.resourceLoader.getResource(
                BASE_DIR
                + CMD1_ID
                + FILE_DELIMITER
                + "config2"
        ).getFile().getAbsolutePath();
        configs.add(configFile1);
        configs.add(configFile2);

        final Set<String> tags = new HashSet<>();
        tags.add("bash");

        final Command cmd = new Command.Builder(
            CMD1_NAME,
            CMD1_USER,
            CMD1_VERSION,
            CommandStatus.ACTIVE,
            "/bin/bash",
            1000
        )
            .withId(CMD1_ID)
            .withSetupFile(setUpFile)
            .withConfigs(configs)
            .withTags(tags)
            .build();

        this.mvc
            .perform(
                MockMvcRequestBuilders
                    .post(COMMANDS_API)
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(OBJECT_MAPPER.writeValueAsBytes(cmd))
            )
            .andExpect(MockMvcResultMatchers.status().isCreated())
            .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
            .andReturn();
    }


    /**
     * Cleanup after tests.
     */
    @After
    public void cleanup() {
        this.jobRequestRepository.deleteAll();
        this.jobRepository.deleteAll();
        this.jobExecutionRepository.deleteAll();
        this.clusterRepository.deleteAll();
        this.commandRepository.deleteAll();
        this.applicationRepository.deleteAll();
    }
    /**
     * Test the job submit method.
     *
     * @throws Exception If there is a problem.
     */
    @Test
    public void testSubmitJobMethod() throws Exception {
            final String commandArgs = "-c 'echo hello world'";

            final List<ClusterCriteria> clusterCriteriaList = new ArrayList<>();
            final Set<String> clusterTags = new HashSet<>();
            clusterTags.add("localhost");
            final ClusterCriteria clusterCriteria = new ClusterCriteria(clusterTags);
            clusterCriteriaList.add(clusterCriteria);

            final Set<String> commandCriteria = new HashSet<>();
            commandCriteria.add("bash");
            final JobRequest jobRequest = new JobRequest.Builder(
                JOB_NAME,
                JOB_USER,
                JOB_VERSION,
                commandArgs,
                clusterCriteriaList,
                commandCriteria
            ).withDisableLogArchival(true)
                .build();

            //final MvcResult result =
                this.mvc
                .perform(
                    MockMvcRequestBuilders
                        .post(JOBS_API)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(OBJECT_MAPPER.writeValueAsBytes(jobRequest))
                )
                .andExpect(MockMvcResultMatchers.status().isAccepted())
                .andExpect(MockMvcResultMatchers.header().string(HttpHeaders.LOCATION, Matchers.notNullValue()))
                .andReturn();

            //final String jobId = this.getIdFromLocation(result.getResponse().getHeader(HttpHeaders.LOCATION));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
                //Handle exception
            }
            log.info("Done");
    }

    private String getIdFromLocation(final String location) {
        return location.substring(location.lastIndexOf("/") + 1);
    }
}
