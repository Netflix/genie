--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.1
-- Dumped by pg_dump version 9.5.1

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
-- Name: application; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE application (
    id character varying(255) NOT NULL,
    created timestamp without time zone,
    updated timestamp without time zone,
    name character varying(255),
    user0 character varying(255),
    version character varying(255),
    envpropfile character varying(255),
    status character varying(20),
    entityversion bigint
);


--
-- Name: application_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE application_configs (
    application_id character varying(255),
    element character varying(255)
);


--
-- Name: application_jars; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE application_jars (
    application_id character varying(255),
    element character varying(255)
);


--
-- Name: application_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE application_tags (
    application_id character varying(255),
    element character varying(255)
);


--
-- Name: cluster; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE cluster (
    id character varying(255) NOT NULL,
    created timestamp without time zone,
    updated timestamp without time zone,
    name character varying(255),
    user0 character varying(255),
    version character varying(255),
    clustertype character varying(255),
    status character varying(20),
    entityversion bigint
);


--
-- Name: cluster_command; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE cluster_command (
    clusters_id character varying(255),
    commands_id character varying(255),
    commands_order integer
);


--
-- Name: cluster_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE cluster_configs (
    cluster_id character varying(255),
    element character varying(255)
);


--
-- Name: cluster_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE cluster_tags (
    cluster_id character varying(255),
    element character varying(255)
);


--
-- Name: command; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE command (
    id character varying(255) NOT NULL,
    created timestamp without time zone,
    updated timestamp without time zone,
    name character varying(255),
    user0 character varying(255),
    version character varying(255),
    envpropfile character varying(255),
    executable character varying(255),
    jobtype character varying(255),
    status character varying(20),
    entityversion bigint,
    application_id character varying(255)
);


--
-- Name: command_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE command_configs (
    command_id character varying(255),
    element character varying(255)
);


--
-- Name: command_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE command_tags (
    command_id character varying(255),
    element character varying(255)
);


--
-- Name: job; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE job (
    id character varying(255) NOT NULL,
    created timestamp without time zone,
    updated timestamp without time zone,
    name character varying(255),
    user0 character varying(255),
    version character varying(255),
    applicationid character varying(255),
    applicationname character varying(255),
    archivelocation text,
    chosenclustercriteriastring text,
    clienthost character varying(255),
    clustercriteriasstring text,
    commandargs text,
    commandcriteriastring text,
    commandid character varying(255),
    commandname character varying(255),
    description character varying(255),
    disablelogarchival boolean,
    email character varying(255),
    envpropfile character varying(255),
    executionclusterid character varying(255),
    executionclustername character varying(255),
    exitcode integer,
    filedependencies text,
    finished timestamp without time zone,
    forwarded boolean,
    groupname character varying(255),
    hostname character varying(255),
    killuri character varying(255),
    outputuri character varying(255),
    processhandle integer,
    started timestamp without time zone,
    status character varying(20),
    statusmsg character varying(255),
    entityversion bigint
);


--
-- Name: job_tags; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE job_tags (
    job_id character varying(255),
    element character varying(255)
);


--
-- Name: application_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY application
    ADD CONSTRAINT application_pkey PRIMARY KEY (id);


--
-- Name: cluster_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY cluster
    ADD CONSTRAINT cluster_pkey PRIMARY KEY (id);


--
-- Name: command_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY command
    ADD CONSTRAINT command_pkey PRIMARY KEY (id);


--
-- Name: job_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY job
    ADD CONSTRAINT job_pkey PRIMARY KEY (id);


--
-- Name: i_clstfgs_cluster_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX i_clstfgs_cluster_id ON cluster_configs USING btree (cluster_id);


--
-- Name: i_clstmnd_clusters_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX i_clstmnd_clusters_id ON cluster_command USING btree (clusters_id);


--
-- Name: i_clstmnd_element; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX i_clstmnd_element ON cluster_command USING btree (commands_id);


--
-- Name: i_clsttgs_cluster_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX i_clsttgs_cluster_id ON cluster_tags USING btree (cluster_id);


--
-- Name: i_cmmnfgs_command_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX i_cmmnfgs_command_id ON command_configs USING btree (command_id);


--
-- Name: i_cmmntgs_command_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX i_cmmntgs_command_id ON command_tags USING btree (command_id);


--
-- Name: i_command_application; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX i_command_application ON command USING btree (application_id);


--
-- Name: i_job_tgs_job_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX i_job_tgs_job_id ON job_tags USING btree (job_id);


--
-- Name: i_pplcfgs_application_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX i_pplcfgs_application_id ON application_configs USING btree (application_id);


--
-- Name: i_pplcjrs_application_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX i_pplcjrs_application_id ON application_jars USING btree (application_id);


--
-- Name: i_pplctgs_application_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX i_pplctgs_application_id ON application_tags USING btree (application_id);


--
-- PostgreSQL database dump complete
--

