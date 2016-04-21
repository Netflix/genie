--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.2
-- Dumped by pg_dump version 9.5.2

SET statement_timeout = 0;
SET lock_timeout = 0;
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
    config character varying(1024) NOT NULL
);


--
-- Name: application_dependencies; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE application_dependencies (
    application_id character varying(255) NOT NULL,
    dependency character varying(1024) NOT NULL
);


--
-- Name: applications; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE applications (
    id character varying(255) NOT NULL,
    created timestamp without time zone DEFAULT now() NOT NULL,
    updated timestamp without time zone DEFAULT now() NOT NULL,
    name character varying(255) NOT NULL,
    "user" character varying(255) NOT NULL,
    version character varying(255) NOT NULL,
    setup_file character varying(1024) DEFAULT NULL::character varying,
    status character varying(20) DEFAULT 'INACTIVE'::character varying NOT NULL,
    entity_version integer DEFAULT 0 NOT NULL,
    description character varying(10000) DEFAULT NULL::character varying,
    tags character varying(2048) DEFAULT NULL::character varying,
    type character varying(255) DEFAULT NULL::character varying
);


--
-- Name: cluster_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE cluster_configs (
    cluster_id character varying(255) NOT NULL,
    config character varying(1024) NOT NULL
);


--
-- Name: clusters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE clusters (
    id character varying(255) NOT NULL,
    created timestamp without time zone DEFAULT now() NOT NULL,
    updated timestamp without time zone DEFAULT now() NOT NULL,
    name character varying(255) NOT NULL,
    "user" character varying(255) NOT NULL,
    version character varying(255) NOT NULL,
    status character varying(20) DEFAULT 'OUT_OF_SERVICE'::character varying NOT NULL,
    entity_version integer DEFAULT 0 NOT NULL,
    description character varying(10000) DEFAULT NULL::character varying,
    tags character varying(2048) DEFAULT NULL::character varying,
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
    config character varying(1024) NOT NULL
);


--
-- Name: commands; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE commands (
    id character varying(255) NOT NULL,
    created timestamp without time zone DEFAULT now() NOT NULL,
    updated timestamp without time zone DEFAULT now() NOT NULL,
    name character varying(255) NOT NULL,
    "user" character varying(255) NOT NULL,
    version character varying(255) NOT NULL,
    setup_file character varying(1024) DEFAULT NULL::character varying,
    executable character varying(255) NOT NULL,
    status character varying(20) DEFAULT 'INACTIVE'::character varying NOT NULL,
    entityversion integer DEFAULT 0 NOT NULL,
    description character varying(10000) DEFAULT NULL::character varying,
    tags character varying(2048) DEFAULT NULL::character varying,
    check_delay bigint DEFAULT 10000 NOT NULL
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
    created timestamp without time zone DEFAULT now() NOT NULL,
    updated timestamp without time zone DEFAULT now() NOT NULL,
    entity_version integer DEFAULT 0 NOT NULL,
    host_name character varying(255) NOT NULL,
    process_id integer NOT NULL,
    exit_code integer DEFAULT '-1'::integer NOT NULL,
    check_delay bigint DEFAULT 10000 NOT NULL,
    timeout timestamp without time zone DEFAULT ((now())::date + 7) NOT NULL
);


--
-- Name: job_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE job_requests (
    id character varying(255) NOT NULL,
    created timestamp without time zone DEFAULT now() NOT NULL,
    updated timestamp without time zone DEFAULT now() NOT NULL,
    name character varying(255) NOT NULL,
    "user" character varying(255) NOT NULL,
    version character varying(255) NOT NULL,
    description character varying(10000) DEFAULT NULL::character varying,
    entity_version integer DEFAULT 0 NOT NULL,
    command_args character varying(10000) NOT NULL,
    group_name character varying(255) DEFAULT NULL::character varying,
    setup_file character varying(1024) DEFAULT NULL::character varying,
    cluster_criterias character varying(2048) DEFAULT '[]'::character varying NOT NULL,
    command_criteria character varying(1024) DEFAULT '[]'::character varying NOT NULL,
    dependencies character varying(30000) DEFAULT NULL::character varying NOT NULL,
    disable_log_archival boolean DEFAULT false NOT NULL,
    email character varying(255) DEFAULT NULL::character varying,
    tags character varying(2048) DEFAULT NULL::character varying,
    cpu integer DEFAULT 1 NOT NULL,
    memory integer DEFAULT 1560 NOT NULL,
    client_host character varying(255) DEFAULT NULL::character varying,
    applications character varying(2048) DEFAULT '[]'::character varying NOT NULL,
    timeout integer DEFAULT 604800 NOT NULL
);


--
-- Name: jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE jobs (
    id character varying(255) NOT NULL,
    created timestamp without time zone DEFAULT now() NOT NULL,
    updated timestamp without time zone DEFAULT now() NOT NULL,
    name character varying(255) NOT NULL,
    "user" character varying(255) NOT NULL,
    version character varying(255) NOT NULL,
    archive_location character varying(1024) DEFAULT NULL::character varying,
    command_args character varying(10000) NOT NULL,
    command_id character varying(255) DEFAULT NULL::character varying,
    command_name character varying(255) DEFAULT NULL::character varying,
    description character varying(10000) DEFAULT NULL::character varying,
    cluster_id character varying(255) DEFAULT NULL::character varying,
    cluster_name character varying(255) DEFAULT NULL::character varying,
    finished timestamp without time zone,
    started timestamp without time zone,
    status character varying(20) DEFAULT 'INIT'::character varying NOT NULL,
    status_msg character varying(255) NOT NULL,
    entityversion integer DEFAULT 0 NOT NULL,
    tags character varying(2048) DEFAULT NULL::character varying
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
-- Name: application_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY applications
    ADD CONSTRAINT application_pkey PRIMARY KEY (id);


--
-- Name: cluster_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY clusters
    ADD CONSTRAINT cluster_pkey PRIMARY KEY (id);


--
-- Name: command_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY commands
    ADD CONSTRAINT command_pkey PRIMARY KEY (id);


--
-- Name: job_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs
    ADD CONSTRAINT job_pkey PRIMARY KEY (id);


--
-- Name: job_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
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
-- Name: jobs_updated_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_updated_index ON jobs USING btree (updated);


--
-- Name: jobs_user_index; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX jobs_user_index ON jobs USING btree ("user");


--
-- Name: application_configs_application_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY application_configs
    ADD CONSTRAINT application_configs_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications(id) ON DELETE CASCADE;


--
-- Name: application_dependencies_application_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY application_dependencies
    ADD CONSTRAINT application_dependencies_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications(id) ON DELETE CASCADE;


--
-- Name: cluster_configs_cluster_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY cluster_configs
    ADD CONSTRAINT cluster_configs_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE;


--
-- Name: clusters_commands_cluster_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY clusters_commands
    ADD CONSTRAINT clusters_commands_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE CASCADE;


--
-- Name: clusters_commands_command_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY clusters_commands
    ADD CONSTRAINT clusters_commands_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands(id) ON DELETE RESTRICT;


--
-- Name: command_configs_command_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY command_configs
    ADD CONSTRAINT command_configs_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands(id) ON DELETE CASCADE;


--
-- Name: commands_applications_application_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY commands_applications
    ADD CONSTRAINT commands_applications_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications(id) ON DELETE RESTRICT;


--
-- Name: commands_applications_command_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY commands_applications
    ADD CONSTRAINT commands_applications_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands(id) ON DELETE CASCADE;


--
-- Name: job_executions_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY job_executions
    ADD CONSTRAINT job_executions_id_fkey FOREIGN KEY (id) REFERENCES jobs(id) ON DELETE CASCADE;


--
-- Name: jobs_applications_application_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs_applications
    ADD CONSTRAINT jobs_applications_application_id_fkey FOREIGN KEY (application_id) REFERENCES applications(id) ON DELETE RESTRICT;


--
-- Name: jobs_applications_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs_applications
    ADD CONSTRAINT jobs_applications_job_id_fkey FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE;


--
-- Name: jobs_cluster_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs
    ADD CONSTRAINT jobs_cluster_id_fkey FOREIGN KEY (cluster_id) REFERENCES clusters(id) ON DELETE RESTRICT;


--
-- Name: jobs_command_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs
    ADD CONSTRAINT jobs_command_id_fkey FOREIGN KEY (command_id) REFERENCES commands(id) ON DELETE RESTRICT;


--
-- Name: jobs_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY jobs
    ADD CONSTRAINT jobs_id_fkey FOREIGN KEY (id) REFERENCES job_requests(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

