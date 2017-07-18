--
-- PostgreSQL database dump
--

-- Dumped from database version 9.6.3
-- Dumped by pg_dump version 9.6.3

-- Started on 2017-07-06 10:33:35 PDT

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

SET default_tablespace = '';

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
    description text DEFAULT NULL,
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
    description text DEFAULT NULL,
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
    description text DEFAULT NULL,
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
    description text DEFAULT NULL,
    entity_version integer DEFAULT 0 NOT NULL,
    command_args character varying(10000) NOT NULL,
    group_name character varying(255) DEFAULT NULL::character varying,
    setup_file character varying(1024) DEFAULT NULL::character varying,
    cluster_criterias text DEFAULT ''::character varying NOT NULL,
    command_criteria text DEFAULT ''::character varying NOT NULL,
    dependencies text DEFAULT ''::character varying NOT NULL,
    configs text DEFAULT ''::character varying NOT NULL,
    disable_log_archival boolean DEFAULT false NOT NULL,
    email character varying(255) DEFAULT NULL::character varying,
    tags character varying(10000) DEFAULT NULL::character varying,
    cpu integer,
    memory integer,
    applications character varying(2048) DEFAULT '[]'::character varying NOT NULL,
    timeout integer
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
    description text DEFAULT NULL,
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
-- Name: applications application_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY applications
    ADD CONSTRAINT application_pkey PRIMARY KEY (id);


--
-- Name: clusters cluster_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY clusters
    ADD CONSTRAINT cluster_pkey PRIMARY KEY (id);


--
-- Name: commands command_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY commands
    ADD CONSTRAINT command_pkey PRIMARY KEY (id);


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
-- Name: job_executions_exit_code_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX job_executions_exit_code_index ON job_executions USING btree (exit_code);


--
-- Name: job_executions_hostname_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX job_executions_hostname_index ON job_executions USING btree (host_name);


--
-- Name: job_requests_created_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX job_requests_created_index ON job_requests USING btree (created);


--
-- Name: jobs_cluster_name_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_cluster_name_index ON jobs USING btree (cluster_name);


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
-- Name: jobs_name_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_name_index ON jobs USING btree (name);


--
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


-- Completed on 2017-07-06 10:33:36 PDT

--
-- PostgreSQL database dump complete
--
