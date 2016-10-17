--
-- PostgreSQL database dump
--

-- Dumped from database version 9.5.4
-- Dumped by pg_dump version 9.5.4

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner:
--



--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner:
--


--
-- Name: r_coll_main; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_coll_main (
    coll_id bigint NOT NULL,
    parent_coll_name character varying(2700) NOT NULL,
    coll_name character varying(2700) NOT NULL,
    coll_owner_name character varying(250) NOT NULL,
    coll_owner_zone character varying(250) NOT NULL,
    coll_map_id bigint DEFAULT 0,
    coll_inheritance character varying(1000),
    coll_type character varying(250) DEFAULT '0',
    coll_info1 character varying(2700) DEFAULT '0',
    coll_info2 character varying(2700) DEFAULT '0',
    coll_expiry_ts character varying(32),
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_data_main; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_data_main (
    data_id bigint NOT NULL,
    coll_id bigint NOT NULL,
    data_name character varying(1000) NOT NULL,
    data_repl_num integer NOT NULL,
    data_version character varying(250) DEFAULT '0',
    data_type_name character varying(250) NOT NULL,
    data_size bigint NOT NULL,
    resc_group_name character varying(250),
    resc_name character varying(250) NOT NULL,
    data_path character varying(2700) NOT NULL,
    data_owner_name character varying(250) NOT NULL,
    data_owner_zone character varying(250) NOT NULL,
    data_is_dirty integer DEFAULT 0,
    data_status character varying(250),
    data_checksum character varying(1000),
    data_expiry_ts character varying(32),
    data_map_id bigint DEFAULT 0,
    data_mode character varying(32),
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32),
    resc_hier character varying(1000),
    resc_id bigint
);



--
-- Name: r_grid_configuration; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_grid_configuration (
    namespace character varying(2700),
    option_name character varying(2700),
    option_value character varying(2700)
);



--
-- Name: r_meta_main; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_meta_main (
    meta_id bigint NOT NULL,
    meta_namespace character varying(250),
    meta_attr_name character varying(2700) NOT NULL,
    meta_attr_value character varying(2700) NOT NULL,
    meta_attr_unit character varying(250),
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_microsrvc_main; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_microsrvc_main (
    msrvc_id bigint NOT NULL,
    msrvc_name character varying(250) NOT NULL,
    msrvc_module_name character varying(250) NOT NULL,
    msrvc_signature character varying(2700) NOT NULL,
    msrvc_doxygen character varying(2500) NOT NULL,
    msrvc_variations character varying(2500) NOT NULL,
    msrvc_owner_name character varying(250) NOT NULL,
    msrvc_owner_zone character varying(250) NOT NULL,
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_microsrvc_ver; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_microsrvc_ver (
    msrvc_id bigint NOT NULL,
    msrvc_version character varying(250) DEFAULT '0',
    msrvc_host character varying(250) DEFAULT 'ALL',
    msrvc_location character varying(500),
    msrvc_language character varying(250) DEFAULT 'C',
    msrvc_type_name character varying(250) DEFAULT 'IRODS COMPILED',
    msrvc_status bigint DEFAULT 1,
    msrvc_owner_name character varying(250) NOT NULL,
    msrvc_owner_zone character varying(250) NOT NULL,
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_objectid; Type: SEQUENCE; Schema: public; Owner: irods
--




--
-- Name: r_objt_access; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_objt_access (
    object_id bigint NOT NULL,
    user_id bigint NOT NULL,
    access_type_id bigint NOT NULL,
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_objt_audit; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_objt_audit (
    object_id bigint NOT NULL,
    user_id bigint NOT NULL,
    action_id bigint NOT NULL,
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_objt_deny_access; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_objt_deny_access (
    object_id bigint NOT NULL,
    user_id bigint NOT NULL,
    access_type_id bigint NOT NULL,
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_objt_metamap; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_objt_metamap (
    object_id bigint NOT NULL,
    meta_id bigint NOT NULL,
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_quota_main; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_quota_main (
    user_id bigint,
    resc_id bigint,
    quota_limit bigint,
    quota_over bigint,
    modify_ts character varying(32)
);



--
-- Name: r_quota_usage; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_quota_usage (
    user_id bigint,
    resc_id bigint,
    quota_usage bigint,
    modify_ts character varying(32)
);



--
-- Name: r_resc_group; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_resc_group (
    resc_group_id bigint NOT NULL,
    resc_group_name character varying(250) NOT NULL,
    resc_id bigint NOT NULL,
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_resc_main; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_resc_main (
    resc_id bigint NOT NULL,
    resc_name character varying(250) NOT NULL,
    zone_name character varying(250) NOT NULL,
    resc_type_name character varying(250) NOT NULL,
    resc_class_name character varying(250) NOT NULL,
    resc_net character varying(250) NOT NULL,
    resc_def_path character varying(1000) NOT NULL,
    free_space character varying(250),
    free_space_ts character varying(32),
    resc_info character varying(1000),
    r_comment character varying(1000),
    resc_status character varying(32),
    create_ts character varying(32),
    modify_ts character varying(32),
    resc_children character varying(1000),
    resc_context character varying(1000),
    resc_parent character varying(1000),
    resc_objcount bigint DEFAULT 0,
    resc_parent_context character varying(4000)
);



--
-- Name: r_rule_base_map; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_rule_base_map (
    map_version character varying(250) DEFAULT '0',
    map_base_name character varying(250) NOT NULL,
    map_priority integer NOT NULL,
    rule_id bigint NOT NULL,
    map_owner_name character varying(250) NOT NULL,
    map_owner_zone character varying(250) NOT NULL,
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_rule_dvm; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_rule_dvm (
    dvm_id bigint NOT NULL,
    dvm_version character varying(250) DEFAULT '0',
    dvm_base_name character varying(250) NOT NULL,
    dvm_ext_var_name character varying(250) NOT NULL,
    dvm_condition character varying(2700),
    dvm_int_map_path character varying(2700) NOT NULL,
    dvm_status integer DEFAULT 1,
    dvm_owner_name character varying(250) NOT NULL,
    dvm_owner_zone character varying(250) NOT NULL,
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_rule_dvm_map; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_rule_dvm_map (
    map_dvm_version character varying(250) DEFAULT '0',
    map_dvm_base_name character varying(250) NOT NULL,
    dvm_id bigint NOT NULL,
    map_owner_name character varying(250) NOT NULL,
    map_owner_zone character varying(250) NOT NULL,
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_rule_exec; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_rule_exec (
    rule_exec_id bigint NOT NULL,
    rule_name character varying(2700) NOT NULL,
    rei_file_path character varying(2700),
    user_name character varying(250),
    exe_address character varying(250),
    exe_time character varying(32),
    exe_frequency character varying(250),
    priority character varying(32),
    estimated_exe_time character varying(32),
    notification_addr character varying(250),
    last_exe_time character varying(32),
    exe_status character varying(32),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_rule_fnm; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_rule_fnm (
    fnm_id bigint NOT NULL,
    fnm_version character varying(250) DEFAULT '0',
    fnm_base_name character varying(250) NOT NULL,
    fnm_ext_func_name character varying(250) NOT NULL,
    fnm_int_func_name character varying(2700) NOT NULL,
    fnm_status integer DEFAULT 1,
    fnm_owner_name character varying(250) NOT NULL,
    fnm_owner_zone character varying(250) NOT NULL,
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_rule_fnm_map; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_rule_fnm_map (
    map_fnm_version character varying(250) DEFAULT '0',
    map_fnm_base_name character varying(250) NOT NULL,
    fnm_id bigint NOT NULL,
    map_owner_name character varying(250) NOT NULL,
    map_owner_zone character varying(250) NOT NULL,
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_rule_main; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_rule_main (
    rule_id bigint NOT NULL,
    rule_version character varying(250) DEFAULT '0',
    rule_base_name character varying(250) NOT NULL,
    rule_name character varying(2700) NOT NULL,
    rule_event character varying(2700) NOT NULL,
    rule_condition character varying(2700),
    rule_body character varying(2700) NOT NULL,
    rule_recovery character varying(2700) NOT NULL,
    rule_status bigint DEFAULT 1,
    rule_owner_name character varying(250) NOT NULL,
    rule_owner_zone character varying(250) NOT NULL,
    rule_descr_1 character varying(2700),
    rule_descr_2 character varying(2700),
    input_params character varying(2700),
    output_params character varying(2700),
    dollar_vars character varying(2700),
    icat_elements character varying(2700),
    sideeffects character varying(2700),
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_server_load; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_server_load (
    host_name character varying(250) NOT NULL,
    resc_name character varying(250) NOT NULL,
    cpu_used integer,
    mem_used integer,
    swap_used integer,
    runq_load integer,
    disk_space integer,
    net_input integer,
    net_output integer,
    create_ts character varying(32)
);



--
-- Name: r_server_load_digest; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_server_load_digest (
    resc_name character varying(250) NOT NULL,
    load_factor integer,
    create_ts character varying(32)
);



--
-- Name: r_specific_query; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_specific_query (
    alias character varying(1000),
    sqlstr character varying(2700),
    create_ts character varying(32)
);



--
-- Name: r_ticket_allowed_groups; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_ticket_allowed_groups (
    ticket_id bigint NOT NULL,
    group_name character varying(250) NOT NULL
);



--
-- Name: r_ticket_allowed_hosts; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_ticket_allowed_hosts (
    ticket_id bigint NOT NULL,
    host character varying(32)
);



--
-- Name: r_ticket_allowed_users; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_ticket_allowed_users (
    ticket_id bigint NOT NULL,
    user_name character varying(250) NOT NULL
);



--
-- Name: r_ticket_main; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_ticket_main (
    ticket_id bigint NOT NULL,
    ticket_string character varying(100),
    ticket_type character varying(20),
    user_id bigint NOT NULL,
    object_id bigint NOT NULL,
    object_type character varying(16),
    uses_limit integer DEFAULT 0,
    uses_count integer DEFAULT 0,
    write_file_limit integer DEFAULT 10,
    write_file_count integer DEFAULT 0,
    write_byte_limit integer DEFAULT 0,
    write_byte_count integer DEFAULT 0,
    ticket_expiry_ts character varying(32),
    restrictions character varying(16),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_tokn_main; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_tokn_main (
    token_namespace character varying(250) NOT NULL,
    token_id bigint NOT NULL,
    token_name character varying(250) NOT NULL,
    token_value character varying(250),
    token_value2 character varying(250),
    token_value3 character varying(250),
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_user_auth; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_user_auth (
    user_id bigint NOT NULL,
    user_auth_name character varying(1000),
    create_ts character varying(32)
);



--
-- Name: r_user_group; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_user_group (
    group_user_id bigint NOT NULL,
    user_id bigint NOT NULL,
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_user_main; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_user_main (
    user_id bigint NOT NULL,
    user_name character varying(250) NOT NULL,
    user_type_name character varying(250) NOT NULL,
    zone_name character varying(250) NOT NULL,
    user_info character varying(1000),
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_user_password; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_user_password (
    user_id bigint NOT NULL,
    rcat_password character varying(250) NOT NULL,
    pass_expiry_ts character varying(32) NOT NULL,
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_user_session_key; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_user_session_key (
    user_id bigint NOT NULL,
    session_key character varying(1000) NOT NULL,
    session_info character varying(1000),
    auth_scheme character varying(250) NOT NULL,
    session_expiry_ts character varying(32) NOT NULL,
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: r_zone_main; Type: TABLE; Schema: public; Owner: irods
--

CREATE TABLE r_zone_main (
    zone_id bigint NOT NULL,
    zone_name character varying(250) NOT NULL,
    zone_type_name character varying(250) NOT NULL,
    zone_conn_string character varying(1000),
    r_comment character varying(1000),
    create_ts character varying(32),
    modify_ts character varying(32)
);



--
-- Name: idx_coll_main1; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_coll_main1 ON r_coll_main USING btree (coll_id);


--
-- Name: idx_coll_main2; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_coll_main2 ON r_coll_main USING btree (parent_coll_name, coll_name);


--
-- Name: idx_coll_main3; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_coll_main3 ON r_coll_main USING btree (coll_name);


--
-- Name: idx_data_main1; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_data_main1 ON r_data_main USING btree (data_id);


--
-- Name: idx_data_main2; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_data_main2 ON r_data_main USING btree (coll_id, data_name, data_repl_num, data_version);


--
-- Name: idx_data_main3; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_data_main3 ON r_data_main USING btree (coll_id);


--
-- Name: idx_data_main4; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_data_main4 ON r_data_main USING btree (data_name);


--
-- Name: idx_data_main5; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_data_main5 ON r_data_main USING btree (data_type_name);


--
-- Name: idx_data_main6; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_data_main6 ON r_data_main USING btree (data_path);


--
-- Name: idx_grid_configuration; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_grid_configuration ON r_grid_configuration USING btree (namespace, option_name);


--
-- Name: idx_meta_main1; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_meta_main1 ON r_meta_main USING btree (meta_id);


--
-- Name: idx_meta_main2; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_meta_main2 ON r_meta_main USING btree (meta_attr_name);


--
-- Name: idx_meta_main3; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_meta_main3 ON r_meta_main USING btree (meta_attr_value);


--
-- Name: idx_meta_main4; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_meta_main4 ON r_meta_main USING btree (meta_attr_unit);


--
-- Name: idx_objt_access1; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_objt_access1 ON r_objt_access USING btree (object_id, user_id);


--
-- Name: idx_objt_daccs1; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_objt_daccs1 ON r_objt_deny_access USING btree (object_id, user_id);


--
-- Name: idx_objt_metamap1; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_objt_metamap1 ON r_objt_metamap USING btree (object_id, meta_id);


--
-- Name: idx_objt_metamap2; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_objt_metamap2 ON r_objt_metamap USING btree (object_id);


--
-- Name: idx_objt_metamap3; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_objt_metamap3 ON r_objt_metamap USING btree (meta_id);


--
-- Name: idx_quota_main1; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_quota_main1 ON r_quota_main USING btree (user_id);


--
-- Name: idx_resc_logical1; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_resc_logical1 ON r_resc_group USING btree (resc_group_name, resc_id);


--
-- Name: idx_resc_main1; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_resc_main1 ON r_resc_main USING btree (resc_id);


--
-- Name: idx_resc_main2; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_resc_main2 ON r_resc_main USING btree (zone_name, resc_name);


--
-- Name: idx_rule_exec; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_rule_exec ON r_rule_exec USING btree (rule_exec_id);


--
-- Name: idx_rule_main1; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_rule_main1 ON r_rule_main USING btree (rule_id);


--
-- Name: idx_specific_query1; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_specific_query1 ON r_specific_query USING btree (sqlstr);


--
-- Name: idx_specific_query2; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_specific_query2 ON r_specific_query USING btree (alias);


--
-- Name: idx_ticket; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_ticket ON r_ticket_main USING btree (ticket_string);


--
-- Name: idx_ticket_group; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_ticket_group ON r_ticket_allowed_groups USING btree (ticket_id, group_name);


--
-- Name: idx_ticket_host; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_ticket_host ON r_ticket_allowed_hosts USING btree (ticket_id, host);


--
-- Name: idx_ticket_user; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_ticket_user ON r_ticket_allowed_users USING btree (ticket_id, user_name);


--
-- Name: idx_tokn_main1; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_tokn_main1 ON r_tokn_main USING btree (token_id);


--
-- Name: idx_tokn_main2; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_tokn_main2 ON r_tokn_main USING btree (token_name);


--
-- Name: idx_tokn_main3; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_tokn_main3 ON r_tokn_main USING btree (token_value);


--
-- Name: idx_tokn_main4; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_tokn_main4 ON r_tokn_main USING btree (token_namespace);


--
-- Name: idx_user_group1; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_user_group1 ON r_user_group USING btree (group_user_id, user_id);


--
-- Name: idx_user_main1; Type: INDEX; Schema: public; Owner: irods
--

CREATE INDEX idx_user_main1 ON r_user_main USING btree (user_id);


--
-- Name: idx_user_main2; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_user_main2 ON r_user_main USING btree (user_name, zone_name);


--
-- Name: idx_zone_main1; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_zone_main1 ON r_zone_main USING btree (zone_id);


--
-- Name: idx_zone_main2; Type: INDEX; Schema: public; Owner: irods
--

CREATE UNIQUE INDEX idx_zone_main2 ON r_zone_main USING btree (zone_name);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--



--
-- PostgreSQL database dump complete
--
