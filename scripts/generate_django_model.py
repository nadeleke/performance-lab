import settings
for factor in settings.CSV_HEADER:
    if (factor.startswith('hw') or factor == 'sw_env_padding') and not factor == 'hw_cpu_arch':
        s = """
class avg_{}(Model):
    class Meta:
        db_table = 'avg_{}'
    experiment_id = columns.Integer(primary_key=True)
    {} = columns.Integer(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()""".format(factor, factor, factor, factor)
        print s
    elif factor.startswith('sw') or factor == 'hw_cpu_arch':
        s = """
class avg_{}(Model):
    class Meta:
        db_table = 'avg_{}'
    experiment_id = columns.Integer(primary_key=True)
    {} = columns.Text(primary_key=True)
    setup_time = columns.Float()
    run_time = columns.Float()
    collect_time = columns.Float()""".format(factor, factor, factor, factor)
        print s

l=[]
for factor in settings.CSV_HEADER:
    if factor.startswith('hw') or factor.startswith('sw'):
        l.append('avg_'+factor)

print 'tables have been generated for ', ', '.join(l)