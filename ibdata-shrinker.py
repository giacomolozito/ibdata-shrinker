#!/usr/bin/env python
#
# ibdata-shrinker for MySQL >= 5.6
# - written by Giacomo Lozito (giacomo.lozito@gmail.com)
#
# A tool to shrink ibdata1 by temporarily removing all InnoDB tables
# from the target MySQL database.
# See README.md for usage details.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

import os, sys
import shutil, stat
import MySQLdb
import argparse, ConfigParser, getpass


class MySqlConn(object):
	def __init__(self, params, exit_if_query_fails=False):
		self.conn_args = { "unix_socket": params["db_socket"] }
		if params["db_user"]:
			self.conn_args["user"] = params["db_user"]
		if params["db_password"]:
			self.conn_args["passwd"] = params["db_password"]
		self.exit_if_query_fails = exit_if_query_fails

	def __enter__(self):
		self.dbconn = MySQLdb.connect(host="localhost", **self.conn_args)
		self.dbcurs = self.dbconn.cursor()
		return self

	def __exit__(self, type, value, traceback):
		self.dbcurs.close()
		self.dbconn.close()

	def query(self, query_str):
		try:
			self.dbcurs.execute(query_str);
		except MySQLdb.ProgrammingError as e:
			if self.exit_if_query_fails:
				sys.stderr.write("\nERROR: a DB error occurred: %s\n" % (e))
				sys.exit(10)
			else:
				raise MySqlConnException(e)
		return self.dbcurs.fetchall()


class MySqlConnException(Exception):
	pass


class IbShrinkException(Exception):
	pass


class bcolors:
	NORMAL = '\033[0m' if not os.environ.get('PYTHON_NOCOLOR') else ''
	GREEN = '\033[32m' if not os.environ.get('PYTHON_NOCOLOR') else ''
	WARNING = '\033[1;33m' if not os.environ.get('PYTHON_NOCOLOR') else ''
	FAIL = '\033[1;31m' if not os.environ.get('PYTHON_NOCOLOR') else ''


def run_stage_pre_export(params):
	with MySqlConn(params, exit_if_query_fails=True) as myconn:
		# 1) get some essential information
		mysql_datadir_result = myconn.query("show global variables like 'datadir'")
		mysql_datadir = mysql_datadir_result[0][1]
		# 2) safety checks:
		#    - if use_hardlink has been specified, ensure that workdir and mysql datadir live in the same filesystem 
		#    - ensure that innodb_file_per_table is enabled
		#    - ensure that the specified workdir is empty
		try:
			if params["use_hardlink"] == "yes" and os.stat(mysql_datadir).st_dev != os.stat(params["workdir"]).st_dev:
				raise IbShrinkException("use_hardlink was specified but MySQL datadir %s and workdir %s "
					"do not live in the same filesystem, aborting" % (mysql_datadir, params["workdir"]))
			fpt_result = myconn.query("show global variables like 'innodb_file_per_table'")
			if fpt_result[0][1] in ('OFF','off',False,'0'):
				raise IbShrinkException("innodb_file_per_table not enabled in database, aborting")
			if os.listdir(params["workdir"]):
				raise IbShrinkException("workdir %s is not empty, delete content if you want to run stage 1 again" % (params["workdir"]))
		except IbShrinkException as e:
			sys.stderr.write(bcolors.NORMAL+"ERROR: %s\n" % str(e))
			sys.exit(5)
		# 3) get a list of all InnoDB tables in mysql schema , write it in workdir file
		inno_mysql_result = myconn.query("""select table_schema,table_name from information_schema.tables 
							where table_schema in ('mysql','sys') and engine = 'innodb'""")
		inno_mysql_list = map(lambda x: "%s.%s\n" % (x[0],x[1]), inno_mysql_result)
		util_write_list_to_file(params["workdir"]+"/inno_list_mysql", inno_mysql_list)
		# 4) get a list of all InnoDB tables in other schemas , write it in workdir file
		inno_apps_result = myconn.query("""select table_schema,table_name from information_schema.tables 
							where table_schema not in ('mysql','information_schema','sys') and engine = 'innodb'""")
		inno_apps_list = map(lambda x: "%s.%s\n" % (x[0],x[1]), inno_apps_result)
		util_write_list_to_file(params["workdir"]+"/inno_list_apps", inno_apps_list)
		# 5) warn user
		sys.stdout.write(bcolors.WARNING+"\nThe following tables will be converted from InnoDB to MyISAM:\n"+bcolors.NORMAL)
		sys.stdout.writelines(inno_mysql_list)
		sys.stdout.write(bcolors.WARNING+"\nThe following tables will be exported from database:\n"+bcolors.NORMAL)
		sys.stdout.writelines(inno_apps_list)
		# all done


def run_stage_export(params):
	with MySqlConn(params, exit_if_query_fails=True) as myconn:
		# 1) get some essential information
		mysql_datadir_result = myconn.query("show global variables like 'datadir'")
		mysql_datadir = mysql_datadir_result[0][1]
		# 2) convert mysql tables from innodb to myisam
		inno_mysql_list = util_read_list_from_file(params["workdir"]+"/inno_list_mysql")
		for table in inno_mysql_list:
			sys.stdout.write(bcolors.NORMAL+"Converting table %s to MyISAM... " % (table))
			myconn.query("alter table %s engine=myisam" % (table))
			sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		# 3) export table structure
		inno_apps_list = util_read_list_from_file(params["workdir"]+"/inno_list_apps")
		for table in inno_apps_list:
			schema_name, table_name = table.split('.', 1)
			target_dir = params["workdir"]+'/'+schema_name
			if not os.path.isdir(target_dir):
				os.mkdir(target_dir)
			sys.stdout.write(bcolors.NORMAL+"Export table definition for table %s ... " % (table))
			table_def_result = myconn.query("show create table %s" % (table))
			table_def = table_def_result[0][1]
			util_write_list_to_file(target_dir+'/'+table_name+'.createtable.sql',(table_def,))
			sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		# 4) export app tables (innodb tablespace)
		sys.stdout.write(bcolors.NORMAL+"\nFlushing tables for export... ")
		myconn.query("flush tables %s for export" % (', '.join(inno_apps_list)))
		sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		for table in inno_apps_list:
			schema_name, table_name = table.split('.', 1)
			target_dir = params["workdir"]+'/'+schema_name
			table_file_cfg = mysql_datadir.rstrip('/')+'/'+schema_name+'/'+table_name+'.cfg'
			table_file_ibd = mysql_datadir.rstrip('/')+'/'+schema_name+'/'+table_name+'.ibd'
			if not os.path.isdir(target_dir):
				os.mkdir(target_dir)
			if not os.path.isfile(table_file_cfg) or not os.path.isfile(table_file_ibd):
				sys.stderr.write(bcolors.NORMAL+"ERROR: file %s was expected but it does not exist!\n" % (table_file_cfg))
				sys.exit(6)
			sys.stdout.write(bcolors.NORMAL+"Copying table cfg for %s in %s... " % (table, target_dir))
			util_copy_preserve_stats(table_file_cfg, target_dir+'/'+os.path.basename(table_file_cfg))
			sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
			if params["use_hardlink"] == "yes":
				sys.stdout.write(bcolors.NORMAL+"Creating hardlink for %s in %s... " % (table, target_dir))
				os.link(table_file_ibd, target_dir+'/'+os.path.basename(table_file_ibd))
				sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
			else:
				sys.stdout.write(bcolors.NORMAL+"Copying table ibd for %s in %s (can take some time)... " % (table, target_dir))
				util_copy_preserve_stats(table_file_ibd, target_dir+'/'+os.path.basename(table_file_ibd))
				sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		# 5) drop innodb app tables from database
		sys.stdout.write(bcolors.NORMAL+"\nDisable foreign key checks for this session... ")
		myconn.query("set foreign_key_checks=0")
		sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		sys.stdout.write(bcolors.NORMAL+"\nUnlock tables and prepare to drop them... ")
		myconn.query("unlock tables")
		sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		for table in inno_apps_list:
			sys.stdout.write(bcolors.NORMAL+"Dropping table %s ... " % (table))
			myconn.query("drop table %s" % (table))
			sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		# 6) warn user
		sys.stdout.write(bcolors.WARNING+"\nAll the exports steps have been executed.\n"
			"You might want to doublecheck that no real InnoDB tables are left in your database.\n"
			"Once this has been confirmed, stop your database and delete the ibdata1 and ib_log* files. Restart your database and ibdata1 "
			"will be re-created.\nLast, run this script again with --stage 2 to run the re-import of tablespaces.\n"
			"Do NOT delete or alter the content of your workdir (or re-run --stage 1) until the re-import has been done "
			"or data will be lost!\n"+bcolors.NORMAL)
		# all done	


def run_stage_import(params):
	with MySqlConn(params, exit_if_query_fails=True) as myconn:
		# 1) import preparation, also check if table list files are in place
		try:
			if not os.path.isdir(params["workdir"]):
				raise IbShrinkException("workdir %s not found, ensure stage 1 has been executed first\n" % (params["workdir"]))
			workdir_filelist = os.listdir(params["workdir"])
			if "inno_list_mysql" not in workdir_filelist or "inno_list_apps" not in workdir_filelist:
				raise IbShrinkException("table list files not found in workdir %s , ensure stage 1 has been executed first\n" % (params["workdir"]))
		except IbShrinkException as e:
			sys.stderr.write("ERROR: %s" % (str(e)))
			sys.exit(7)
		mysql_datadir_result = myconn.query("show global variables like 'datadir'")
		mysql_datadir = mysql_datadir_result[0][1]
		sys.stdout.write(bcolors.NORMAL+"\nDisable foreign key checks for this session... ")
		myconn.query("set foreign_key_checks=0")
		sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		# 2) convert mysql tables from myisam to innodb
		inno_mysql_list = util_read_list_from_file(params["workdir"]+"/inno_list_mysql")
		for table in inno_mysql_list:
			sys.stdout.write(bcolors.NORMAL+"Converting back table %s to InnoDB... " % (table))
			myconn.query("alter table %s engine=innodb" % (table))
			sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		# 3) re-create innodb app tables
		inno_apps_list = util_read_list_from_file(params["workdir"]+"/inno_list_apps")
		for table in inno_apps_list:
			schema_name, table_name = table.split('.', 1)
			source_dir = params["workdir"]+'/'+schema_name
			sys.stdout.write(bcolors.NORMAL+"Creating back table %s ... " % (table))
			createtable_sql = ' '.join(util_read_list_from_file(source_dir+'/'+table_name+'.createtable.sql'))
			myconn.query("use %s" % (schema_name))
			myconn.query("%s" % (createtable_sql))
			sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		# 4) discard tablespace on newly-created tables, in preparation of import
		for table in inno_apps_list:
			sys.stdout.write(bcolors.NORMAL+"Discarding new tablespace on table %s ... " % (table))
			myconn.query("alter table %s discard tablespace" % (table))
			sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		# 5) copy back cfg and ibd files from the workdir
		for table in inno_apps_list:
			schema_name, table_name = table.split('.', 1)
			source_dir = params["workdir"]+'/'+schema_name
			target_dir = mysql_datadir.rstrip('/')+'/'+schema_name
			table_file_cfg = source_dir+'/'+table_name+'.cfg'
			table_file_ibd = source_dir+'/'+table_name+'.ibd'
			sys.stdout.write(bcolors.NORMAL+"Copying back table cfg for %s in %s... " % (table, target_dir))
			util_copy_preserve_stats(table_file_cfg, target_dir+'/'+os.path.basename(table_file_cfg))
			sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
			if params["use_hardlink"] == "yes":
				sys.stdout.write(bcolors.NORMAL+"Creating back hardlink for %s in %s... " % (table, target_dir))
				os.link(table_file_ibd, target_dir+'/'+os.path.basename(table_file_ibd))
				sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
			else:
				sys.stdout.write(bcolors.NORMAL+"Copying back table ibd for %s in %s (can take some time)... " % (table, target_dir))
				util_copy_preserve_stats(table_file_ibd, target_dir+'/'+os.path.basename(table_file_ibd))
				sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		# 6) import tablespaces
		for table in inno_apps_list:
			sys.stdout.write(bcolors.NORMAL+"Importing old tablespace on table %s ... " % (table))
			myconn.query("alter table %s import tablespace" % (table))
			sys.stdout.write(bcolors.GREEN+"OK\n"+bcolors.NORMAL)
		# 7) warn user
		sys.stdout.write((bcolors.WARNING+"\nAll the import steps have been executed!\n"
			"You might want to check that your InnoDB tables are back in place along with their data.\n"
			"Once this has been confirmed, your workdir %s can be removed from system.\n"+bcolors.NORMAL) % (params["workdir"]))
		# all done


def util_copy_preserve_stats(source_file, dest_file):
	stats = os.stat(source_file)
	shutil.copy2(source_file, dest_file)
	os.chown(dest_file, stats.st_uid, stats.st_gid)

def util_write_list_to_file(target_file, lines):
	with open(target_file,'w') as f:
		f.writelines(lines)

def util_read_list_from_file(target_file):
	lines = ()
	with open(target_file,'r') as f:
		lines = [line.strip('\n') for line in f]
	return lines

def util_get_user_ok_to_proceed():
	choice = None
	while choice not in ("yes","no"):
		sys.stdout.write("Do you want to proceed? Type yes or no\n")
		choice = raw_input().lower()
	return choice


def util_read_config(config_file, profile):
	config = ConfigParser.RawConfigParser()
	config.read(config_file)
	mandatory_params = {"db_socket":None, "workdir":None}
	optional_params = {"use_hardlink":False, "db_user":None, "db_password":None}
	for opt,val in mandatory_params.iteritems():
		if config.has_option(profile, opt):
			mandatory_params[opt] = config.get(profile, opt)
		else:
			raise IbShrinkException("Mandatory parameter %s not found in config file %s profile %s" % (opt, config_file, profile))
	for opt,val in optional_params.iteritems():
		if config.has_option(profile, opt):
			optional_params[opt] = config.get(profile, opt)
	mandatory_params.update(optional_params)
	return mandatory_params


if __name__ == "__main__":
	parser = argparse.ArgumentParser(formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=40))
	parser.add_argument("-c","--config", help="configuration file", required=True)
	parser.add_argument("-p","--profile", help="profile to use in configuration file (default: \"default\")", default="default")
	parser.add_argument("-s","--stage", help="stage of operation (1=export, 2=import)", type=int, choices=(1,2), required=True)
	parser.add_argument("-P","--password", help="type db password interactively", action="store_true")
	args = parser.parse_args()

	if not os.path.isfile(args.config):
		sys.stderr.write("ERROR: file %s not found, aborting\n" % (args.config))
		sys.exit(1)

	try:
		params = util_read_config(args.config, args.profile)
	except IbShrinkException as e:
		sys.stderr.write("ERROR: %s\n" % (e))
		sys.exit(2)
	# params check
	if not os.path.isdir(params["workdir"]):
		sys.stderr.write("ERROR: workdir defined in directory %s but it does not exist, aborting\n" % (params["workdir"]))
		sys.exit(3)
	if not os.path.exists(params["db_socket"]):
		sys.stderr.write("ERROR: database socket %s does not exist, aborting\n" % (params["db_socket"]))
		sys.exit(4)

	if args.password:
		params["db_password"] = getpass.getpass("Enter database password for %s profile: " % (args.profile))

	if args.stage == 1:
		# collect preliminary information
		run_stage_pre_export(params)
		# check if user is OK with pre_export
		sys.stdout.write(bcolors.WARNING+"\nMake sure to check the list above and ensure ")
		sys.stdout.write("there are no connections to this database during this procedure!\n"+bcolors.NORMAL)
		if ( util_get_user_ok_to_proceed() == "no" ):
			# cleanup workdir and exit
			for filename in os.listdir(params["workdir"]):
				if os.path.isfile(params["workdir"]+'/'+filename):
					os.unlink(params["workdir"]+'/'+filename)
			sys.stdout.write("Exiting now\n")
			sys.exit(0)
		# proceed with the export
		run_stage_export(params)
		# all done
	elif args.stage == 2:
		# proceed with the import
		run_stage_import(params)
		# all done
