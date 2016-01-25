import uuid
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model

class Log(Model):
    time      = columns.DateTime(primary_key=True)
    level    = columns.Text()
    message      = columns.Text()