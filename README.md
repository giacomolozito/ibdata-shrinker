# ibdata-shrinker
A tool to shrink ibdata1 in MySQL databases, using transportable tablespaces to temporarily remove InnoDB tables.  
This tool is experimental - use it at your own risk and always take a backup of your database first.  
  
Ibdata1 is a core datafile for InnoDB in MySQL, containing a lot of information (data dictionary, undo space, etc.). Unfortunately, a known issue of this file is that it cannot be shrunk once it's grown larger in size, i.e. because of a long-lasting transaction that filled the undo log with data while running. The ibdata1 file can be deleted and re-created but this requires the InnoDB tables to be exported first with tools like mysqldump, something that might be unfeasible for big databases (hundreds of GB or more).

Ibdata-shrinker tries to work around the issue by taking advantage of [transportable tablespaces](https://dev.mysql.com/doc/refman/5.6/en/glossary.html#glos_transportable_tablespace) introduced in MySQL 5.6.  
Its strategy consists of two automated steps and a manual one:
- first automated stage of operation, in which InnoDB application tables are exported in a temporary location and internal InnoDB tables (mysql.\*, sys.\*) are converted to MyISAM
- once that is done, the user can manually stop the database, remove ibdata1 and ib_log\* files, and start the database again (which will trigger ibdata and ib_log* files re-creation with an initial size of choice)
- last, the tool is launched in the second stage of operation, automated, in which internal tables are converted back to InnoDB and application tables are imported again

## Requirements
- python2 >= 2.6 , with modules MySQLdb and argparse
- MySQL >= 5.6 (tested with 5.6 and 5.7), with innodb_file_per_table used for all InnoDB tables; all [limits of transportable tablespaces](http://dev.mysql.com/doc/refman/5.6/en/tablespace-copying.html) apply to this tool
- tool must be executed locally on the database machine, connecting to db via UNIX socket
- ensure the user running the tool has read/write permission in the MySQL datadir
- ensure that no changes are done to database while the tool is running (i.e. replication, other connections, etc.); a good approach is to isolate the database with --skip-networking and --skip-slave-start

## Usage
Ibdata-shrinker requires the definition of a INI-like config file containing the essential information.  
This is an example config file:
```ini
[default]
workdir=/tmp/my_workdir
db_socket=/var/lib/mysql/mysql.sock
db_user=mypoweruser
db_password=1234
use_hardlink=no

[another_db]
workdir=/tmp/another_workdir
db_socket=/var/lib/mysql/another-mysql.sock
```
At the very least, workdir and db_socket must be present. Detail of the parameters:
- **workdir** : the directory used to contain database information and the exported InnoDB tablespaces
- **db_socket** : the UNIX socket for the target database
- **db_user** : (optional, default blank) defines the user for database connections; user must have enough privileges to be able to read all database tables, alter them and export/import tablespaces; if not sure, grant all privileges to this user
- **db_password** : (optional, default blank) defines the password for the database user; it is also possible to use the **-P** parameter when launching the tool to specify the password interactively rather than writing it in the config file
- **use_hardlink** : (optional, default no) if set to yes, InnoDB data files are hardlinked in the workdir rather than copied; this can be a lot  faster and does not require any extra disk space for InnoDB tablespaces; it requires the workdir and the MySQL datadir to be in the same filesystem

It is possible to describe multiple databases in the config file using different [profiles]. The tool uses one specific profile when running (by default it looks for the 'default' profile, the **-p** option controls the chosen profile).

Once the configuration file has been defined, ibdata-shrinker can be used as follows:
```bash
# launch stage 1 using default profile from my_config_file
./ibdata-shrinker -c my_config_file -p default -s 1
# stage 1 will list all the tables that will have to be converted or exported and require user confirmation;
# once user confirms the list to be correct, it will execute and export/convert accordingly

# once stage 1 is complete, remove innodb core files from database
# your method of stopping/starting database might vary (and your MySQL datadir could be in a different location)
service mysql stop
rm -rf /var/lib/mysql/ibdata1 /var/lib/mysql/ib_log*
service mysql start

# on start, ibdata1 and ib_logfiles are re-created; launc stage 2 to reimport the exported InnoDB tables
./ibdata-shrinker -c my_config_file -p default -s 2

# once this is complete, altered internal tables will have been converted back from MyISAM to InnoDB
# and tablespaces will have been imported again
# verify that your data is in place
```

## Suggestions
- ibdata-shrinker is experimental! -Always- take a backup of your database before running
- to help preventing ibdata1 file from growing big again, MySQL 5.6 and especially 5.7 provide [some new options that allow to move undo tablespaces into separate files](http://dev.mysql.com/doc/refman/5.6/en/innodb-parameters.html#sysvar_innodb_undo_tablespaces)
