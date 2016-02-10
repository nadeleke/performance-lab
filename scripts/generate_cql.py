import settings
for factor in settings.CSV_HEADER:
    if factor.startswith('hw') or factor.startswith('sw'):
        print "DROP TABLE avg_{};".format(factor)

for factor in settings.CSV_HEADER:
    if (factor.startswith('hw') or factor == 'sw_env_padding') and not factor == 'hw_cpu_arch':
        print "CREATE TABLE avg_{} (experiment_id int, {} int, setup_time double, run_time double, collect_time double, PRIMARY KEY (experiment_id, {}));".format(factor, factor, factor)
    elif factor.startswith('sw') or factor == 'hw_cpu_arch':
        print "CREATE TABLE avg_{} (experiment_id int, {} text, setup_time double, run_time double, collect_time double, PRIMARY KEY (experiment_id, {}));".format(factor, factor, factor)
