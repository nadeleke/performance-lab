CREATE KEYSPACE datamill WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
USE datamill;
CREATE TABLE log (time timestamp, level text, message text, PRIMARY KEY (time));
