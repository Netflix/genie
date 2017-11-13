/*
 *
 *  Copyright 2017 Netflix, Inc.
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

--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.5
-- Dumped by pg_dump version 9.6.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_with_oids = false;

--
-- Name: application_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE application_configs (
    application_id character varying(255) NOT NULL,
    config character varying(2048) NOT NULL
);


--
-- Name: application_dependencies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE application_dependencies (
    application_id character varying(255) NOT NULL,
    dependency character varying(2048) NOT NULL
);


--
-- Name: applications; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE applications (
    id character varying(255) NOT NULL,
    created timestamp(3) without time zone DEFAULT now() NOT NULL,
    updated timestamp(3) without time zone DEFAULT now() NOT NULL,
    name character varying(255) NOT NULL,
    genie_user character varying(255) NOT NULL,
    version character varying(255) NOT NULL,
    setup_file character varying(1024) DEFAULT NULL::character varying,
    status character varying(20) DEFAULT 'INACTIVE'::character varying NOT NULL,
    entity_version integer DEFAULT 0 NOT NULL,
    description text,
    tags character varying(10000) DEFAULT NULL::character varying,
    type character varying(255) DEFAULT NULL::character varying
);


--
-- Name: cluster_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE cluster_configs (
    cluster_id character varying(255) NOT NULL,
    config character varying(2048) NOT NULL
);


--
-- Name: cluster_dependencies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE cluster_dependencies (
    cluster_id character varying(255) NOT NULL,
    dependency character varying(2048) NOT NULL
);


--
-- Name: clusters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE clusters (
    id character varying(255) NOT NULL,
    created timestamp(3) without time zone DEFAULT now() NOT NULL,
    updated timestamp(3) without time zone DEFAULT now() NOT NULL,
    name character varying(255) NOT NULL,
    genie_user character varying(255) NOT NULL,
    version character varying(255) NOT NULL,
    status character varying(20) DEFAULT 'OUT_OF_SERVICE'::character varying NOT NULL,
    entity_version integer DEFAULT 0 NOT NULL,
    description text,
    tags character varying(10000) DEFAULT NULL::character varying,
    setup_file character varying(1024) DEFAULT NULL::character varying
);


--
-- Name: clusters_commands; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE clusters_commands (
    cluster_id character varying(255) NOT NULL,
    command_id character varying(255) NOT NULL,
    command_order integer NOT NULL
);


--
-- Name: command_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE command_configs (
    command_id character varying(255) NOT NULL,
    config character varying(2048) NOT NULL
);


--
-- Name: command_dependencies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE command_dependencies (
    command_id character varying(255) NOT NULL,
    dependency character varying(2048) NOT NULL
);


--
-- Name: commands; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE commands (
    id character varying(255) NOT NULL,
    created timestamp(3) without time zone DEFAULT now() NOT NULL,
    updated timestamp(3) without time zone DEFAULT now() NOT NULL,
    name character varying(255) NOT NULL,
    genie_user character varying(255) NOT NULL,
    version character varying(255) NOT NULL,
    setup_file character varying(1024) DEFAULT NULL::character varying,
    executable character varying(255) NOT NULL,
    status character varying(20) DEFAULT 'INACTIVE'::character varying NOT NULL,
    entity_version integer DEFAULT 0 NOT NULL,
    description text,
    tags character varying(10000) DEFAULT NULL::character varying,
    check_delay bigint DEFAULT 10000 NOT NULL,
    memory integer
);


--
-- Name: commands_applications; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE commands_applications (
    command_id character varying(255) NOT NULL,
    application_id character varying(255) NOT NULL,
    application_order integer NOT NULL
);


--
-- Name: job_executions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE job_executions (
    id character varying(255) NOT NULL,
    created timestamp(3) without time zone DEFAULT now() NOT NULL,
    updated timestamp(3) without time zone DEFAULT now() NOT NULL,
    entity_version integer DEFAULT 0 NOT NULL,
    host_name character varying(255) NOT NULL,
    process_id integer,
    exit_code integer,
    check_delay bigint,
    timeout timestamp without time zone,
    memory integer
);


--
-- Name: job_metadata; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE job_metadata (
    id character varying(255) NOT NULL,
    created timestamp(3) without time zone DEFAULT now() NOT NULL,
    updated timestamp(3) without time zone DEFAULT now() NOT NULL,
    entity_version integer DEFAULT 0 NOT NULL,
    client_host character varying(255) DEFAULT NULL::character varying,
    user_agent character varying(2048) DEFAULT NULL::character varying,
    num_attachments integer,
    total_size_of_attachments bigint,
    std_out_size bigint,
    std_err_size bigint
);


--
-- Name: job_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE job_requests (
    id character varying(255) NOT NULL,
    created timestamp(3) without time zone DEFAULT now() NOT NULL,
    updated timestamp(3) without time zone DEFAULT now() NOT NULL,
    name character varying(255) NOT NULL,
    genie_user character varying(255) NOT NULL,
    version character varying(255) NOT NULL,
    description text,
    entity_version integer DEFAULT 0 NOT NULL,
    command_args character varying(10000) NOT NULL,
    group_name character varying(255) DEFAULT NULL::character varying,
    setup_file character varying(1024) DEFAULT NULL::character varying,
    cluster_criterias text DEFAULT ''::character varying NOT NULL,
    command_criteria text DEFAULT ''::character varying NOT NULL,
    dependencies text DEFAULT ''::character varying NOT NULL,
    disable_log_archival boolean DEFAULT false NOT NULL,
    email character varying(255) DEFAULT NULL::character varying,
    tags character varying(10000) DEFAULT NULL::character varying,
    cpu integer,
    memory integer,
    applications character varying(2048) DEFAULT '[]'::character varying NOT NULL,
    timeout integer,
    configs text DEFAULT ''::character varying NOT NULL
);


--
-- Name: jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE jobs (
    id character varying(255) NOT NULL,
    created timestamp(3) without time zone DEFAULT now() NOT NULL,
    updated timestamp(3) without time zone DEFAULT now() NOT NULL,
    name character varying(255) NOT NULL,
    genie_user character varying(255) NOT NULL,
    version character varying(255) NOT NULL,
    archive_location character varying(1024) DEFAULT NULL::character varying,
    command_args character varying(10000) NOT NULL,
    command_id character varying(255) DEFAULT NULL::character varying,
    command_name character varying(255) DEFAULT NULL::character varying,
    description text,
    cluster_id character varying(255) DEFAULT NULL::character varying,
    cluster_name character varying(255) DEFAULT NULL::character varying,
    finished timestamp(3) without time zone DEFAULT NULL::timestamp without time zone,
    started timestamp(3) without time zone DEFAULT NULL::timestamp without time zone,
    status character varying(20) DEFAULT 'INIT'::character varying NOT NULL,
    status_msg character varying(255) NOT NULL,
    entity_version integer DEFAULT 0 NOT NULL,
    tags character varying(10000) DEFAULT NULL::character varying
);


--
-- Name: jobs_applications; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE jobs_applications (
    job_id character varying(255) NOT NULL,
    application_id character varying(255) NOT NULL,
    application_order integer NOT NULL
);


--
-- Name: application_configs application_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY application_configs
    ADD CONSTRAINT application_configs_pkey PRIMARY KEY (application_id, config);


--
-- Name: application_dependencies application_dependencies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY application_dependencies
    ADD CONSTRAINT application_dependencies_pkey PRIMARY KEY (application_id, dependency);


--
-- Name: applications application_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY applications
    ADD CONSTRAINT application_pkey PRIMARY KEY (id);


--
-- Name: cluster_configs cluster_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY cluster_configs
    ADD CONSTRAINT cluster_configs_pkey PRIMARY KEY (cluster_id, config);


--
-- Name: cluster_dependencies cluster_dependency_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY cluster_dependencies
    ADD CONSTRAINT cluster_dependency_pkey PRIMARY KEY (cluster_id, dependency);


--
-- Name: clusters cluster_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY clusters
    ADD CONSTRAINT cluster_pkey PRIMARY KEY (id);


--
-- Name: clusters_commands clusters_commands_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY clusters_commands
    ADD CONSTRAINT clusters_commands_pkey PRIMARY KEY (cluster_id, command_id, command_order);


--
-- Name: command_configs command_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY command_configs
    ADD CONSTRAINT command_configs_pkey PRIMARY KEY (command_id, config);


--
-- Name: command_dependencies command_dependency_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY command_dependencies
    ADD CONSTRAINT command_dependency_pkey PRIMARY KEY (command_id, dependency);


--
-- Name: commands command_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY commands
    ADD CONSTRAINT command_pkey PRIMARY KEY (id);


--
-- Name: commands_applications commands_applications_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY commands_applications
    ADD CONSTRAINT commands_applications_pkey PRIMARY KEY (command_id, application_id, application_order);


--
-- Name: job_executions job_executions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY job_executions
    ADD CONSTRAINT job_executions_pkey PRIMARY KEY (id);


--
-- Name: job_metadata job_metadata_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY job_metadata
    ADD CONSTRAINT job_metadata_pkey PRIMARY KEY (id);


--
-- Name: jobs job_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs
    ADD CONSTRAINT job_pkey PRIMARY KEY (id);


--
-- Name: job_requests job_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY job_requests
    ADD CONSTRAINT job_requests_pkey PRIMARY KEY (id);


--
-- Name: jobs_applications jobs_applications_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs_applications
    ADD CONSTRAINT jobs_applications_pkey PRIMARY KEY (job_id, application_id, application_order);


--
-- Name: application_configs_application_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX application_configs_application_id_index ON application_configs USING btree (application_id);


--
-- Name: application_dependencies_application_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX application_dependencies_application_id_index ON application_dependencies USING btree (application_id);


--
-- Name: applications_name_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX applications_name_index ON applications USING btree (name);


--
-- Name: applications_status_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX applications_status_index ON applications USING btree (status);


--
-- Name: applications_tags_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX applications_tags_index ON applications USING btree (tags);


--
-- Name: applications_type_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX applications_type_index ON applications USING btree (type);


--
-- Name: cluster_configs_cluster_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cluster_configs_cluster_id_index ON cluster_configs USING btree (cluster_id);


--
-- Name: cluster_dependencies_cluster_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cluster_dependencies_cluster_id_index ON cluster_dependencies USING btree (cluster_id);


--
-- Name: clusters_commands_cluster_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX clusters_commands_cluster_id_index ON clusters_commands USING btree (cluster_id);


--
-- Name: clusters_commands_command_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX clusters_commands_command_id_index ON clusters_commands USING btree (command_id);


--
-- Name: clusters_name_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX clusters_name_index ON clusters USING btree (name);


--
-- Name: clusters_status_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX clusters_status_index ON clusters USING btree (status);


--
-- Name: clusters_tag_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX clusters_tag_index ON clusters USING btree (tags);


--
-- Name: command_configs_command_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX command_configs_command_id_index ON command_configs USING btree (command_id);


--
-- Name: command_dependencies_command_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX command_dependencies_command_id_index ON command_dependencies USING btree (command_id);


--
-- Name: commands_applications_application_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX commands_applications_application_id_index ON commands_applications USING btree (application_id);


--
-- Name: commands_applications_command_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX commands_applications_command_id_index ON commands_applications USING btree (command_id);


--
-- Name: commands_name_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX commands_name_index ON commands USING btree (name);


--
-- Name: commands_status_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX commands_status_index ON commands USING btree (status);


--
-- Name: commands_tags_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX commands_tags_index ON commands USING btree (tags);


--
-- Name: job_executions_hostname_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX job_executions_hostname_index ON job_executions USING btree (host_name);


--
-- Name: job_requests_created_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX job_requests_created_index ON job_requests USING btree (created);


--
-- Name: jobs_applications_application_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_applications_application_id_index ON jobs_applications USING btree (application_id);


--
-- Name: jobs_applications_job_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_applications_job_id_index ON jobs_applications USING btree (job_id);


--
-- Name: jobs_cluster_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_cluster_id_index ON jobs USING btree (cluster_id);


--
-- Name: jobs_cluster_name_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_cluster_name_index ON jobs USING btree (cluster_name);


--
-- Name: jobs_command_id_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_command_id_index ON jobs USING btree (command_id);


--
-- Name: jobs_command_name_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_command_name_index ON jobs USING btree (command_name);


--
-- Name: jobs_created_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_created_index ON jobs USING btree (created);


--
-- Name: jobs_finished_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_finished_index ON jobs USING btree (finished);


--
-- Name: jobs_name_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_name_index ON jobs USING btree (name);


--
-- Name: jobs_started_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_started_index ON jobs USING btree (started);


--
-- Name: jobs_status_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_status_index ON jobs USING btree (status);


--
-- Name: jobs_tags_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_tags_index ON jobs USING btree (tags);


--
-- Name: jobs_user_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_user_index ON jobs USING btree (genie_user);


--
-- Name: application_configs application_configs_application_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY application_configs
    ADD CONSTRAINT application_configs_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications(id) ON DELETE CASCADE;


--
-- Name: application_dependencies application_dependencies_application_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY application_dependencies
    ADD CONSTRAINT application_dependencies_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications(id) ON DELETE CASCADE;


--
-- Name: cluster_configs cluster_configs_cluster_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY cluster_configs
    ADD CONSTRAINT cluster_configs_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE;


--
-- Name: cluster_dependencies cluster_dependencies_cluster_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY cluster_dependencies
    ADD CONSTRAINT cluster_dependencies_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE;


--
-- Name: clusters_commands clusters_commands_cluster_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY clusters_commands
    ADD CONSTRAINT clusters_commands_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE;


--
-- Name: clusters_commands clusters_commands_command_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY clusters_commands
    ADD CONSTRAINT clusters_commands_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands(id) ON DELETE RESTRICT;


--
-- Name: command_configs command_configs_command_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY command_configs
    ADD CONSTRAINT command_configs_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands(id) ON DELETE CASCADE;


--
-- Name: command_dependencies command_dependencies_command_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY command_dependencies
    ADD CONSTRAINT command_dependencies_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands(id) ON DELETE CASCADE;


--
-- Name: commands_applications commands_applications_application_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY commands_applications
    ADD CONSTRAINT commands_applications_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications(id) ON DELETE RESTRICT;


--
-- Name: commands_applications commands_applications_command_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY commands_applications
    ADD CONSTRAINT commands_applications_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands(id) ON DELETE CASCADE;


--
-- Name: job_executions job_executions_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY job_executions
    ADD CONSTRAINT job_executions_id_fkey FOREIGN KEY (id) REFERENCES jobs(id) ON DELETE CASCADE;


--
-- Name: job_metadata job_metadata_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY job_metadata
    ADD CONSTRAINT job_metadata_id_fkey FOREIGN KEY (id) REFERENCES job_requests(id) ON DELETE CASCADE;


--
-- Name: jobs_applications jobs_applications_application_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs_applications
    ADD CONSTRAINT jobs_applications_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications(id) ON DELETE RESTRICT;


--
-- Name: jobs_applications jobs_applications_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs_applications
    ADD CONSTRAINT jobs_applications_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE;


--
-- Name: jobs jobs_cluster_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs
    ADD CONSTRAINT jobs_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE RESTRICT;


--
-- Name: jobs jobs_command_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs
    ADD CONSTRAINT jobs_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands(id) ON DELETE RESTRICT;


--
-- Name: jobs jobs_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs
    ADD CONSTRAINT jobs_id_fkey FOREIGN KEY (id) REFERENCES job_requests(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

