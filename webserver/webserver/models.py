import uuid
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

class avg_hw_cpu_arch(Model):
    class Meta:
        db_table = 'avg_hw_cpu_arch'
    experiment_id = columns.Integer(primary_key=True)
    hw_cpu_arch = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_hw_cpu_mhz(Model):
    class Meta:
        db_table = 'avg_hw_cpu_mhz'
    experiment_id = columns.Integer(primary_key=True)
    hw_cpu_mhz = columns.Integer(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_hw_gpu_mhz(Model):
    class Meta:
        db_table = 'avg_hw_gpu_mhz'
    experiment_id = columns.Integer(primary_key=True)
    hw_gpu_mhz = columns.Integer(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_hw_num_cpus(Model):
    class Meta:
        db_table = 'avg_hw_num_cpus'
    experiment_id = columns.Integer(primary_key=True)
    hw_num_cpus = columns.Integer(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_hw_page_sz(Model):
    class Meta:
        db_table = 'avg_hw_page_sz'
    experiment_id = columns.Integer(primary_key=True)
    hw_page_sz = columns.Integer(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_hw_ram_mhz(Model):
    class Meta:
        db_table = 'avg_hw_ram_mhz'
    experiment_id = columns.Integer(primary_key=True)
    hw_ram_mhz = columns.Integer(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_hw_ram_sz(Model):
    class Meta:
        db_table = 'avg_hw_ram_sz'
    experiment_id = columns.Integer(primary_key=True)
    hw_ram_sz = columns.Integer(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_sw_address_randomization(Model):
    class Meta:
        db_table = 'avg_sw_address_randomization'
    experiment_id = columns.Integer(primary_key=True)
    sw_address_randomization = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_sw_autogroup(Model):
    class Meta:
        db_table = 'avg_sw_autogroup'
    experiment_id = columns.Integer(primary_key=True)
    sw_autogroup = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_sw_compiler(Model):
    class Meta:
        db_table = 'avg_sw_compiler'
    experiment_id = columns.Integer(primary_key=True)
    sw_compiler = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_sw_drop_caches(Model):
    class Meta:
        db_table = 'avg_sw_drop_caches'
    experiment_id = columns.Integer(primary_key=True)
    sw_drop_caches = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_sw_env_padding(Model):
    class Meta:
        db_table = 'avg_sw_env_padding'
    experiment_id = columns.Integer(primary_key=True)
    sw_env_padding = columns.Integer(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_sw_filesystem(Model):
    class Meta:
        db_table = 'avg_sw_filesystem'
    experiment_id = columns.Integer(primary_key=True)
    sw_filesystem = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_sw_freq_scaling(Model):
    class Meta:
        db_table = 'avg_sw_freq_scaling'
    experiment_id = columns.Integer(primary_key=True)
    sw_freq_scaling = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_sw_link_order(Model):
    class Meta:
        db_table = 'avg_sw_link_order'
    experiment_id = columns.Integer(primary_key=True)
    sw_link_order = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_sw_opt_flag(Model):
    class Meta:
        db_table = 'avg_sw_opt_flag'
    experiment_id = columns.Integer(primary_key=True)
    sw_opt_flag = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_sw_swap(Model):
    class Meta:
        db_table = 'avg_sw_swap'
    experiment_id = columns.Integer(primary_key=True)
    sw_swap = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()

class avg_sw_sys_time(Model):
    class Meta:
        db_table = 'avg_sw_sys_time'
    experiment_id = columns.Integer(primary_key=True)
    sw_sys_time = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()
