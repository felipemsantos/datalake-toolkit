USE db_mdb_raw_dev;

CREATE EXTERNAL TABLE tb_call_req(
    id int,
    persid varchar(30),
    ref_num varchar(30),
    summary varchar(255),
    description string,
    status varchar(12),
    active_flag int,
    open_date int,
    time_spent_sum int,
    last_mod_dt int,
    last_mod_by string,
    close_date int,
    resolve_date int,
    rootcause int,
    log_agent string,
    assignee string,
    group_id string,
    customer string,
    charge_back_id varchar(12),
    affected_rc string,
    support_lev varchar(30),
    category varchar(30),
    solution varchar(30),
    impact int,
    priority int,
    urgency int,
    severity int,
    extern_ref varchar(30),
    last_act_id varchar(12),
    cr_tticket int,
    parent varchar(30),
    template_name varchar(30),
    sla_violation int,
    predicted_sla_viol int,
    macro_predict_viol int,
    created_via int,
    call_back_date int,
    call_back_flag int,
    event_token varchar(80),
    sched_token varchar(128),
    type varchar(10),
    string1 varchar(40),
    string2 varchar(40),
    string3 varchar(40),
    string4 varchar(40),
    string5 varchar(40),
    string6 varchar(40),
    problem varchar(30),
    incident_priority int,
    change int,
    ticket_avoided int,
    tenant string,
    cawf_procid varchar(40),
    caused_by_chg int,
    outage_start_time int,
    outage_end_time int,
    affected_service string,
    external_system_ticket string,
    incorrectly_assigned int,
    major_incident int,
    orig_user_admin_org float,
    orig_user_cost_center int,
    orig_user_dept int,
    orig_user_organization string,
    outage_detail_what string,
    outage_detail_who string,
    outage_detail_why string,
    outage_reason_desc string,
    outage_type int,
    pct_service_restored int,
    remote_control_used int,
    requested_by string,
    resolution_code int,
    resolution_method int,
    resolvable_at_lower int,
    return_to_service int,
    symptom_code int,
    target_closed_count int,
    target_closed_last int,
    target_hold_count int,
    target_hold_last int,
    target_resolved_count int,
    target_resolved_last int,
    target_start_last int,
    caextwf_instance_id int,
    sap_category varchar(32),
    sap_client varchar(3),
    sap_component varchar(32),
    sap_cprog varchar(24),
    sap_dbsys varchar(12),
    sap_frontend varchar(12),
    sap_instance varchar(12),
    sap_msg varchar(12),
    sap_os varchar(32),
    sap_priority int,
    sap_sftwcomp varchar(32),
    sap_sftwcomppatch varchar(10),
    sap_sftwcomprel varchar(32),
    sap_solman varchar(1),
    sap_state int,
    sap_status varchar(32),
    sap_subject varchar(32),
    sap_syshost varchar(64),
    sap_sysid varchar(3),
    sap_systyp varchar(12),
    sap_userstatus varchar(40),
    sap_xnum varchar(32),
    fcr int,
    z_srl_aplicacao int,
    z_str_posicao varchar(50),
    z_str_regional varchar(50),
    z_CanalAbertura int,
    z_lrel_ClienteBpo string,
    z_srl_plataforma int,
    z_srl_tipoProblema int,
    z_lrel_Location string,
    z_srl_FirstCallRes int,
    z_str_Fornecedor varchar(100),
    z_str_FornecedorDesc varchar(1000),
    z_str_FornecedorNum varchar(50),
    z_srl_ChangeRequired int,
    z_str_CatalogNum varchar(60),
    z_dur_FornecedorTime int,
    z_srl_Fornecedor int,
    z_srl_Predio int,
    z_str_JustificativaSLA varchar(500),
    z_dte_LastStatusChange int,
    z_dte_ViolationDate int,
    z_srl_ApproverContact string,
    z_srl_SolutionAnalyst string,
    z_srl_ViolationGroup string,
    z_srl_ResponsavelSolucao string,
    heat int,
    call_back_text varchar(255),
    reminder_duration int,
    start_date int,
    est_comp_date int,
    actual_comp_date int,
    z_str_cpf varchar(200)
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://customer-bigdata-raw-dev/servicedesk/customer/ca_sdm/tb_call_req/latest/';