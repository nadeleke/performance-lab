import settings
for factor in settings.CSV_HEADER:
    if factor.startswith('hw') or factor.startswith('sw'):
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