for factor in 'experiment_id,job_id,results_file,package_id,package_name,worker_id,config_id,replicate_no,setup_time,run_time,collect_time,hw_cpu_arch,hw_cpu_mhz,hw_gpu_mhz,hw_num_cpus,hw_page_sz,hw_ram_mhz,hw_ram_sz,sw_address_randomization,sw_autogroup,sw_compiler,sw_drop_caches,sw_env_padding,sw_filesystem,sw_freq_scaling,sw_link_order,sw_opt_flag,sw_swap,sw_sys_time'.split(','):
    if factor.startswith('hw') or factor.startswith('sw'):
        print "DROP TABLE avg_{};".format(factor)

for factor in 'experiment_id,job_id,results_file,package_id,package_name,worker_id,config_id,replicate_no,setup_time,run_time,collect_time,hw_cpu_arch,hw_cpu_mhz,hw_gpu_mhz,hw_num_cpus,hw_page_sz,hw_ram_mhz,hw_ram_sz,sw_address_randomization,sw_autogroup,sw_compiler,sw_drop_caches,sw_env_padding,sw_filesystem,sw_freq_scaling,sw_link_order,sw_opt_flag,sw_swap,sw_sys_time'.split(','):
    if (factor.startswith('hw') or factor == 'sw_env_padding') and not factor == 'hw_cpu_arch':
        print "CREATE TABLE avg_{} (experiment_id int, {} int, setup_time double, run_time double, collect_time double, PRIMARY KEY (experiment_id, {}));".format(factor, factor, factor)
    elif factor.startswith('sw') or factor == 'hw_cpu_arch':
        print "CREATE TABLE avg_{} (experiment_id int, {} text, setup_time double, run_time double, collect_time double, PRIMARY KEY (experiment_id, {}));".format(factor, factor, factor)
