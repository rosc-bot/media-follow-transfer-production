--
-- PostgreSQL database dump
--

\restrict scHTgK0AjgbEdxFDuLCXfjbzPAvh6AgzbRPfq9rRb1Acnf8oJIcd6QNf9pREojz

-- Dumped from database version 16.15
-- Dumped by pg_dump version 16.15

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: admin_audit_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.admin_audit_log (
    id integer NOT NULL,
    actor_user_id bigint NOT NULL,
    actor_role character varying(16) NOT NULL,
    action character varying(64) NOT NULL,
    target_user_id bigint,
    target_task_id integer,
    before json,
    after json,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: admin_audit_log_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.admin_audit_log_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: admin_audit_log_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.admin_audit_log_id_seq OWNED BY public.admin_audit_log.id;


--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


--
-- Name: app_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.app_settings (
    id integer NOT NULL,
    key character varying(128) NOT NULL,
    value text,
    is_secret_ref boolean NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: app_settings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.app_settings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: app_settings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.app_settings_id_seq OWNED BY public.app_settings.id;


--
-- Name: auto_ingest_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.auto_ingest_history (
    id integer NOT NULL,
    title character varying(512) NOT NULL,
    season integer NOT NULL,
    episodes json,
    share_url character varying(2048),
    provider character varying(255),
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: auto_ingest_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.auto_ingest_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: auto_ingest_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.auto_ingest_history_id_seq OWNED BY public.auto_ingest_history.id;


--
-- Name: bot_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bot_settings (
    key character varying(255) NOT NULL,
    val character varying(4096),
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: channel_ingest_jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_ingest_jobs (
    id integer NOT NULL,
    channel_id character varying(128) NOT NULL,
    message_id integer NOT NULL,
    source_type character varying(32) NOT NULL,
    is_forward boolean NOT NULL,
    share_url text,
    share_hash character varying(128),
    status character varying(32) NOT NULL,
    parsed_data json NOT NULL,
    media_type character varying(32),
    tmdb_id integer,
    title character varying(512),
    year integer,
    season integer,
    detected_episodes json NOT NULL,
    identity_status character varying(32),
    ready_for_transfer boolean NOT NULL,
    transfer_status character varying(32),
    error_message text,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: channel_ingest_jobs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.channel_ingest_jobs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: channel_ingest_jobs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.channel_ingest_jobs_id_seq OWNED BY public.channel_ingest_jobs.id;


--
-- Name: channel_ingest_messages; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_ingest_messages (
    id integer NOT NULL,
    channel_id character varying(128) NOT NULL,
    message_id integer NOT NULL,
    source_type character varying(32) NOT NULL,
    is_forward boolean NOT NULL,
    payload json NOT NULL,
    status character varying(32) NOT NULL,
    created_at timestamp with time zone NOT NULL
);


--
-- Name: channel_ingest_messages_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.channel_ingest_messages_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: channel_ingest_messages_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.channel_ingest_messages_id_seq OWNED BY public.channel_ingest_messages.id;


--
-- Name: channel_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.channel_settings (
    id integer NOT NULL,
    channel_id character varying(128) NOT NULL,
    channel_name character varying(512),
    enabled boolean NOT NULL,
    role character varying(32) NOT NULL,
    transfer_mode character varying(32) NOT NULL,
    accept_forward boolean NOT NULL,
    default_provider character varying(64),
    default_category character varying(128),
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: channel_settings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.channel_settings_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: channel_settings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.channel_settings_id_seq OWNED BY public.channel_settings.id;


--
-- Name: cloud_configs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cloud_configs (
    id integer NOT NULL,
    name character varying(64) NOT NULL,
    domain_pattern character varying(256),
    auth_ref text,
    target_folder_id character varying(256),
    enabled boolean NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    channel_id character varying(64),
    ongoing_target_folder_id character varying(256)
);


--
-- Name: cloud_configs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.cloud_configs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: cloud_configs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.cloud_configs_id_seq OWNED BY public.cloud_configs.id;


--
-- Name: cloud_disk_inventory; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cloud_disk_inventory (
    id integer NOT NULL,
    title character varying(255) NOT NULL,
    clean_title character varying(255) NOT NULL,
    season integer DEFAULT 1 NOT NULL,
    tmdb_id integer,
    episode integer NOT NULL,
    file_name character varying(512) NOT NULL,
    rel_path character varying(1024),
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP NOT NULL
);


--
-- Name: cloud_disk_inventory_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.cloud_disk_inventory_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: cloud_disk_inventory_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.cloud_disk_inventory_id_seq OWNED BY public.cloud_disk_inventory.id;


--
-- Name: failed_scout_pushes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.failed_scout_pushes (
    id integer NOT NULL,
    title character varying(512) NOT NULL,
    season integer DEFAULT 1 NOT NULL,
    episodes json,
    share_url character varying(2048) NOT NULL,
    provider character varying(255),
    text_context text,
    error_message text,
    status character varying(32) DEFAULT 'FAILED'::character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: failed_scout_pushes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.failed_scout_pushes_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: failed_scout_pushes_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.failed_scout_pushes_id_seq OWNED BY public.failed_scout_pushes.id;


--
-- Name: ignored_missing; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ignored_missing (
    id integer NOT NULL,
    title character varying(512) NOT NULL,
    season integer DEFAULT 1 NOT NULL,
    episode integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: ignored_missing_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ignored_missing_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ignored_missing_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ignored_missing_id_seq OWNED BY public.ignored_missing.id;


--
-- Name: resource_candidates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resource_candidates (
    id integer NOT NULL,
    tmdb_id integer NOT NULL,
    title character varying(512) NOT NULL,
    year integer,
    season integer NOT NULL,
    episode_key character varying(64) NOT NULL,
    provider character varying(64) NOT NULL,
    share_url text NOT NULL,
    share_hash character varying(128) NOT NULL,
    source_type character varying(32),
    source_channel_id character varying(128),
    source_message_id integer,
    resource_id integer,
    queue_task_id integer,
    status character varying(32) NOT NULL,
    failure_category character varying(64),
    failure_reason text,
    discovered_at timestamp with time zone NOT NULL,
    last_checked_at timestamp with time zone,
    last_used_at timestamp with time zone,
    attempt_count integer NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: resource_candidates_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.resource_candidates_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: resource_candidates_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.resource_candidates_id_seq OWNED BY public.resource_candidates.id;


--
-- Name: resources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.resources (
    id integer NOT NULL,
    identity_key character varying(512) NOT NULL,
    tmdb_id integer,
    title character varying(512),
    media_type character varying(32) NOT NULL,
    year integer,
    season integer,
    episode integer,
    episode_key character varying(64),
    version_key character varying(128),
    cloud_name character varying(64),
    share_url text NOT NULL,
    source_type character varying(32) NOT NULL,
    source_channel_id character varying(128),
    source_message_id integer,
    status character varying(32) NOT NULL,
    file_names json NOT NULL,
    transferred_folder_id character varying(256),
    created_at timestamp with time zone NOT NULL,
    accepted_at timestamp with time zone
);


--
-- Name: resources_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.resources_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: resources_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.resources_id_seq OWNED BY public.resources.id;


--
-- Name: series_watchlist; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.series_watchlist (
    id integer NOT NULL,
    tmdb_id integer NOT NULL,
    title character varying(512) NOT NULL,
    year integer,
    media_type character varying(32) NOT NULL,
    season integer NOT NULL,
    status character varying(32) NOT NULL,
    follow_mode character varying(32) NOT NULL,
    total_episodes integer,
    last_aired_episode integer,
    collected_episodes json NOT NULL,
    poster_path character varying(1024),
    source character varying(128),
    subscriber_tg_id bigint,
    last_sync_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    tmdb_series_status character varying(64),
    remote_series_folder_id character varying(256),
    remote_destination_kind character varying(32)
);


--
-- Name: series_watchlist_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.series_watchlist_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: series_watchlist_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.series_watchlist_id_seq OWNED BY public.series_watchlist.id;


--
-- Name: telegram_admins; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.telegram_admins (
    id integer NOT NULL,
    telegram_user_id bigint NOT NULL,
    username character varying(255),
    display_name character varying(512),
    role character varying(16) DEFAULT 'ADMIN'::character varying NOT NULL,
    enabled boolean DEFAULT true NOT NULL,
    note text,
    created_by bigint,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone,
    receive_failure_notifications boolean DEFAULT true NOT NULL,
    CONSTRAINT ck_telegram_admin_role CHECK (((role)::text = ANY ((ARRAY['OWNER'::character varying, 'ADMIN'::character varying])::text[])))
);


--
-- Name: telegram_admins_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.telegram_admins_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: telegram_admins_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.telegram_admins_id_seq OWNED BY public.telegram_admins.id;


--
-- Name: telegram_users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.telegram_users (
    id integer NOT NULL,
    telegram_user_id bigint NOT NULL,
    username character varying(255),
    display_name character varying(512),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: telegram_users_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.telegram_users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: telegram_users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.telegram_users_id_seq OWNED BY public.telegram_users.id;


--
-- Name: transfer_jobs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.transfer_jobs (
    id integer NOT NULL,
    resource_id integer NOT NULL,
    provider character varying(64) NOT NULL,
    status character varying(32) NOT NULL,
    target_folder_id character varying(256),
    expected_files json NOT NULL,
    result json NOT NULL,
    attempt_count integer NOT NULL,
    max_retries integer NOT NULL,
    last_error text,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: transfer_jobs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.transfer_jobs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transfer_jobs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.transfer_jobs_id_seq OWNED BY public.transfer_jobs.id;


--
-- Name: transfer_queue_tasks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.transfer_queue_tasks (
    id integer NOT NULL,
    task_type character varying(64) NOT NULL,
    resource_id integer NOT NULL,
    idempotency_key character varying(512) NOT NULL,
    status character varying(32) NOT NULL,
    priority integer NOT NULL,
    payload json NOT NULL,
    result json NOT NULL,
    error_message text,
    attempt_count integer NOT NULL,
    max_retries integer NOT NULL,
    next_run_at timestamp with time zone NOT NULL,
    locked_at timestamp with time zone,
    locked_by character varying(128),
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);


--
-- Name: transfer_queue_tasks_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.transfer_queue_tasks_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: transfer_queue_tasks_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.transfer_queue_tasks_id_seq OWNED BY public.transfer_queue_tasks.id;


--
-- Name: admin_audit_log id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin_audit_log ALTER COLUMN id SET DEFAULT nextval('public.admin_audit_log_id_seq'::regclass);


--
-- Name: app_settings id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.app_settings ALTER COLUMN id SET DEFAULT nextval('public.app_settings_id_seq'::regclass);


--
-- Name: auto_ingest_history id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auto_ingest_history ALTER COLUMN id SET DEFAULT nextval('public.auto_ingest_history_id_seq'::regclass);


--
-- Name: channel_ingest_jobs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ingest_jobs ALTER COLUMN id SET DEFAULT nextval('public.channel_ingest_jobs_id_seq'::regclass);


--
-- Name: channel_ingest_messages id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ingest_messages ALTER COLUMN id SET DEFAULT nextval('public.channel_ingest_messages_id_seq'::regclass);


--
-- Name: channel_settings id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_settings ALTER COLUMN id SET DEFAULT nextval('public.channel_settings_id_seq'::regclass);


--
-- Name: cloud_configs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cloud_configs ALTER COLUMN id SET DEFAULT nextval('public.cloud_configs_id_seq'::regclass);


--
-- Name: cloud_disk_inventory id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cloud_disk_inventory ALTER COLUMN id SET DEFAULT nextval('public.cloud_disk_inventory_id_seq'::regclass);


--
-- Name: failed_scout_pushes id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.failed_scout_pushes ALTER COLUMN id SET DEFAULT nextval('public.failed_scout_pushes_id_seq'::regclass);


--
-- Name: ignored_missing id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ignored_missing ALTER COLUMN id SET DEFAULT nextval('public.ignored_missing_id_seq'::regclass);


--
-- Name: resource_candidates id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_candidates ALTER COLUMN id SET DEFAULT nextval('public.resource_candidates_id_seq'::regclass);


--
-- Name: resources id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources ALTER COLUMN id SET DEFAULT nextval('public.resources_id_seq'::regclass);


--
-- Name: series_watchlist id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_watchlist ALTER COLUMN id SET DEFAULT nextval('public.series_watchlist_id_seq'::regclass);


--
-- Name: telegram_admins id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.telegram_admins ALTER COLUMN id SET DEFAULT nextval('public.telegram_admins_id_seq'::regclass);


--
-- Name: telegram_users id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.telegram_users ALTER COLUMN id SET DEFAULT nextval('public.telegram_users_id_seq'::regclass);


--
-- Name: transfer_jobs id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.transfer_jobs ALTER COLUMN id SET DEFAULT nextval('public.transfer_jobs_id_seq'::regclass);


--
-- Name: transfer_queue_tasks id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.transfer_queue_tasks ALTER COLUMN id SET DEFAULT nextval('public.transfer_queue_tasks_id_seq'::regclass);


--
-- Name: admin_audit_log admin_audit_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.admin_audit_log
    ADD CONSTRAINT admin_audit_log_pkey PRIMARY KEY (id);


--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);


--
-- Name: app_settings app_settings_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.app_settings
    ADD CONSTRAINT app_settings_key_key UNIQUE (key);


--
-- Name: app_settings app_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.app_settings
    ADD CONSTRAINT app_settings_pkey PRIMARY KEY (id);


--
-- Name: auto_ingest_history auto_ingest_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auto_ingest_history
    ADD CONSTRAINT auto_ingest_history_pkey PRIMARY KEY (id);


--
-- Name: bot_settings bot_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bot_settings
    ADD CONSTRAINT bot_settings_pkey PRIMARY KEY (key);


--
-- Name: channel_ingest_jobs channel_ingest_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ingest_jobs
    ADD CONSTRAINT channel_ingest_jobs_pkey PRIMARY KEY (id);


--
-- Name: channel_ingest_messages channel_ingest_messages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ingest_messages
    ADD CONSTRAINT channel_ingest_messages_pkey PRIMARY KEY (id);


--
-- Name: channel_settings channel_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_settings
    ADD CONSTRAINT channel_settings_pkey PRIMARY KEY (id);


--
-- Name: cloud_configs cloud_configs_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cloud_configs
    ADD CONSTRAINT cloud_configs_name_key UNIQUE (name);


--
-- Name: cloud_configs cloud_configs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cloud_configs
    ADD CONSTRAINT cloud_configs_pkey PRIMARY KEY (id);


--
-- Name: cloud_disk_inventory cloud_disk_inventory_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cloud_disk_inventory
    ADD CONSTRAINT cloud_disk_inventory_pkey PRIMARY KEY (id);


--
-- Name: failed_scout_pushes failed_scout_pushes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.failed_scout_pushes
    ADD CONSTRAINT failed_scout_pushes_pkey PRIMARY KEY (id);


--
-- Name: ignored_missing ignored_missing_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ignored_missing
    ADD CONSTRAINT ignored_missing_pkey PRIMARY KEY (id);


--
-- Name: resource_candidates resource_candidates_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_candidates
    ADD CONSTRAINT resource_candidates_pkey PRIMARY KEY (id);


--
-- Name: resources resources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resources
    ADD CONSTRAINT resources_pkey PRIMARY KEY (id);


--
-- Name: series_watchlist series_watchlist_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_watchlist
    ADD CONSTRAINT series_watchlist_pkey PRIMARY KEY (id);


--
-- Name: telegram_admins telegram_admins_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.telegram_admins
    ADD CONSTRAINT telegram_admins_pkey PRIMARY KEY (id);


--
-- Name: telegram_users telegram_users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.telegram_users
    ADD CONSTRAINT telegram_users_pkey PRIMARY KEY (id);


--
-- Name: transfer_jobs transfer_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.transfer_jobs
    ADD CONSTRAINT transfer_jobs_pkey PRIMARY KEY (id);


--
-- Name: transfer_queue_tasks transfer_queue_tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.transfer_queue_tasks
    ADD CONSTRAINT transfer_queue_tasks_pkey PRIMARY KEY (id);


--
-- Name: auto_ingest_history uq_auto_ingest_title_season_url; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.auto_ingest_history
    ADD CONSTRAINT uq_auto_ingest_title_season_url UNIQUE (title, season, share_url);


--
-- Name: resource_candidates uq_candidate_episode_hash; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.resource_candidates
    ADD CONSTRAINT uq_candidate_episode_hash UNIQUE (tmdb_id, season, episode_key, share_hash);


--
-- Name: channel_settings uq_channel_setting_id; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_settings
    ADD CONSTRAINT uq_channel_setting_id UNIQUE (channel_id);


--
-- Name: ignored_missing uq_ignored_missing_title_season_ep; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ignored_missing
    ADD CONSTRAINT uq_ignored_missing_title_season_ep UNIQUE (title, season, episode);


--
-- Name: channel_ingest_jobs uq_ingest_job_source; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ingest_jobs
    ADD CONSTRAINT uq_ingest_job_source UNIQUE (channel_id, message_id, share_hash);


--
-- Name: channel_ingest_messages uq_ingest_message_source; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.channel_ingest_messages
    ADD CONSTRAINT uq_ingest_message_source UNIQUE (channel_id, message_id);


--
-- Name: cloud_disk_inventory uq_inventory_item; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cloud_disk_inventory
    ADD CONSTRAINT uq_inventory_item UNIQUE (clean_title, tmdb_id, season, episode);


--
-- Name: telegram_admins uq_telegram_admin_user_id; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.telegram_admins
    ADD CONSTRAINT uq_telegram_admin_user_id UNIQUE (telegram_user_id);


--
-- Name: telegram_users uq_telegram_user_id; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.telegram_users
    ADD CONSTRAINT uq_telegram_user_id UNIQUE (telegram_user_id);


--
-- Name: transfer_queue_tasks uq_transfer_queue_idempotency; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.transfer_queue_tasks
    ADD CONSTRAINT uq_transfer_queue_idempotency UNIQUE (idempotency_key);


--
-- Name: series_watchlist uq_watchlist_series_subscriber; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.series_watchlist
    ADD CONSTRAINT uq_watchlist_series_subscriber UNIQUE (tmdb_id, season, subscriber_tg_id);


--
-- Name: ix_admin_audit_actor_action; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_admin_audit_actor_action ON public.admin_audit_log USING btree (actor_user_id, action);


--
-- Name: ix_admin_audit_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_admin_audit_created_at ON public.admin_audit_log USING btree (created_at);


--
-- Name: ix_candidate_episode_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_candidate_episode_status ON public.resource_candidates USING btree (tmdb_id, season, episode_key, status);


--
-- Name: ix_candidate_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_candidate_hash ON public.resource_candidates USING btree (share_hash);


--
-- Name: ix_channel_enabled_role; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_channel_enabled_role ON public.channel_settings USING btree (enabled, role);


--
-- Name: ix_channel_ingest_jobs_share_hash; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_channel_ingest_jobs_share_hash ON public.channel_ingest_jobs USING btree (share_hash);


--
-- Name: ix_ingest_job_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_ingest_job_status ON public.channel_ingest_jobs USING btree (status, created_at);


--
-- Name: ix_inventory_clean_title; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_inventory_clean_title ON public.cloud_disk_inventory USING btree (clean_title);


--
-- Name: ix_inventory_tmdb_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_inventory_tmdb_id ON public.cloud_disk_inventory USING btree (tmdb_id);


--
-- Name: ix_resource_episode; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_resource_episode ON public.resources USING btree (tmdb_id, season, episode);


--
-- Name: ix_telegram_admin_enabled_role; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_telegram_admin_enabled_role ON public.telegram_admins USING btree (enabled, role);


--
-- Name: ix_transfer_job_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_transfer_job_status ON public.transfer_jobs USING btree (status, updated_at);


--
-- Name: ix_transfer_queue_claim; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_transfer_queue_claim ON public.transfer_queue_tasks USING btree (status, next_run_at, priority);


--
-- Name: ix_watchlist_status_updated; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_watchlist_status_updated ON public.series_watchlist USING btree (status, updated_at);


--
-- Name: uq_resource_live_identity_key; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_resource_live_identity_key ON public.resources USING btree (identity_key) WHERE ((status)::text <> ALL ((ARRAY['FAILED'::character varying, 'REJECTED'::character varying, 'INVALID'::character varying, 'EXPIRED'::character varying])::text[]));


--
-- Name: uq_telegram_single_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX uq_telegram_single_owner ON public.telegram_admins USING btree (role) WHERE ((role)::text = 'OWNER'::text);


--
-- PostgreSQL database dump complete
--

\unrestrict scHTgK0AjgbEdxFDuLCXfjbzPAvh6AgzbRPfq9rRb1Acnf8oJIcd6QNf9pREojz

