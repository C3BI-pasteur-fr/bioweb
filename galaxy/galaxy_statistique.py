"""
Created on Jan. 30, 2015

@authors: Fabien Mareuil, Institut Pasteur, Paris
@contacts: fabien.mareuil@pasteur.fr
@project: bioweb_galaxy
@githuborganization: bioweb
"""
import ConfigParser
import argparse
import psycopg2
import string
import json
from sqlalchemy import create_engine, select, between, MetaData, func, desc, distinct
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from pwd import getpwnam, getpwuid
from grp import getgrgid
from operator import itemgetter

def map_database(connection):
    """
        Database mapping
    """
    engine = create_engine(connection)
    metadata = MetaData()
    metadata.reflect(engine)
    Base = automap_base(metadata=metadata)
    Base.prepare()
    return Base.classes, engine

def groupby(users_stat):
    stat_group = {}
    for row in users_stat:
        listrow = row.values()
        login = string.split(row[0],"@")[0]
        try:
            user_uid = getpwnam(login).pw_uid
            user_gname = getgrgid(getpwuid(user_uid).pw_gid).gr_name
            if user_gname in stat_group:
                stat_group[user_gname] = stat_group[user_gname] + row[1]
            else:
                stat_group[user_gname] = row[1]
        except KeyError:
            print "User %s doesn't exist in this system" % login
            pass
    return sorted(stat_group.items(), key=itemgetter(1))

def jobs_count(database, engine, date_in, date_out):
    """
        build a count list of tools using by user
    """
    jobs_stats_bytools = []
    jobs_stats_byusers = []
    jobs_stats_usersbytools = []
    
    job, galaxy_user= database.job, database.galaxy_user

    sele= select([job.tool_id, func.count(job.tool_id)]) \
            .where(between(job.create_time, date_in, date_out)) \
            .group_by(job.tool_id) \
            .order_by(desc(func.count(job.tool_id)))

    sele2= select([galaxy_user.email, func.count(job.user_id)]) \
            .where(galaxy_user.id==job.user_id) \
            .where(between(job.create_time, date_in, date_out)) \
            .group_by(galaxy_user.email) \
            .order_by(desc(func.count(job.user_id)))
            
#    sele3= select([job.tool_id, func.count(distinct(job.user_id)), func.min(job.create_time)]) \
    sele3= select([job.tool_id, func.count(distinct(job.user_id))]) \
            .where(between(job.create_time, date_in, date_out)) \
            .group_by(job.tool_id) \
            .order_by(desc(func.count(distinct(job.user_id))))
           
    with engine.connect() as conn:
        result = conn.execute(sele)
        for row in result:
            jobs_stats_bytools.append(row)
    with engine.connect() as conn:
        result = conn.execute(sele2)
        for row in result:
           jobs_stats_byusers.append(row)
    with engine.connect() as conn:
        result = conn.execute(sele3)
        for row in result:
           jobs_stats_usersbytools.append(row)
                 
    bygroup =  groupby(jobs_stats_byusers)

    return jobs_stats_bytools, jobs_stats_byusers, bygroup, jobs_stats_usersbytools 

def config_parsing(configfile):
    """
        Parse the config file
    """
    config = ConfigParser.ConfigParser()
    config.read(configfile)
    db_connection = config.get('app:main', 'database_connection')
    return db_connection

def json_write(output_json, build_dict):
    """
       dump the json 
    """
    with open(output_json, 'w') as jsonfile:
        try:
            json.dump(build_dict, jsonfile, indent=4)
        except SystemExit:
            pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--universefile", help="config file of galaxy")
    parser.add_argument("--date_in", help="format is YYYY-MM-DD")
    parser.add_argument("--date_out", help="format is YYYY-MM-DD")
    parser.add_argument("--module_file", help="module_conf_file")
    parser.add_argument("--bioweb_json_file", help="json bioweb output file")
    args = parser.parse_args()


    database_connection = config_parsing(args.universefile)
    database, engine = map_database(database_connection)
    STAT_BYTOOLS, STAT_BYUSERS, STAT_BYGROUPS, STAT_USERSBYTOOLS = jobs_count(database, engine, args.date_in, args.date_out)

    
    STAT_DIC = {"STAT_BYTOOLS" : [], "STAT_BYUSERS" : [], "STAT_BYGROUPS" : STAT_BYGROUPS, "USERS_BY_TOOLS" : []}
    for row in STAT_BYTOOLS:
        listrow = row.values()
        STAT_DIC["STAT_BYTOOLS"].append((listrow[0], listrow[1]))
        print "%s\t%d" % (listrow[0], listrow[1])
    for row in STAT_BYUSERS:
        listrow = row.values()
        STAT_DIC["STAT_BYUSERS"].append((listrow[0], listrow[1]))
        print "%s\t%d" % (listrow[0], listrow[1])
    for row in STAT_BYGROUPS:
        print "%s\t%d" % (row[0], row[1])
    for row in STAT_USERSBYTOOLS:
        listrow = row.values()
#        STAT_DIC["USERS_BY_TOOLS"].append((listrow[0], listrow[1], listrow[2].strftime("%d/%m/%y")))
#        print "%s\t%d\t%s" % (listrow[0], listrow[1], listrow[2].strftime("%d/%m/%y"))
        STAT_DIC["USERS_BY_TOOLS"].append((listrow[0], listrow[1]))
        print "%s\t%d" % (listrow[0], listrow[1])
        
    json_write(args.bioweb_json_file, STAT_DIC)
    
