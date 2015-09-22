"""
Created on Jan. 30, 2015

@authors: Fabien Mareuil, Institut Pasteur, Paris
@contacts: fabien.mareuil@pasteur.fr
@project: bioweb_galaxy
@githuborganization: bioweb
"""

from sqlalchemy import create_engine, select, between, MetaData, func, desc
from sqlalchemy.ext.automap import automap_base
from pwd import getpwnam, getpwuid
from grp import getgrgid
from operator import itemgetter
import ConfigParser
import argparse
import string
import sys
import json
import pprint
import subprocess



def build_xml_to_dict(module_conf_data):
    """
        return a dictionnary of module_conf.xml
    """
    import xml.etree.ElementTree as ET
    module_dict = {}
    tree = ET.parse(module_conf_data)
    root = tree.getroot()
    for child in root:
        module_dict[child.attrib['id']] = [
            string.split(child.attrib['module']),
            string.split(child.attrib['commands'], ";")]
    return module_dict

def build_modules_names(tool, tools_dict):
    """
        return a list of base names for _id, package and packages_uses, the last
        in the list is the most important
    """
    base = []
    if tools_dict.has_key(tool[0]):
        for module in tools_dict[tool[0]][0]:
            name = module.replace("/", "@")
            #bof mais j'ai pas mieux
            if module in ["ptools/0.99a", "ptools/0.99"]:
                [name] + base
            else:
                base.append(name)
        if not tool[1] in ["Installed"]:
            print  >> sys.stderr, \
                 "WARNING Installed Error %s status is %s" % (tool[0], tool[1])
    else:
        if tool[1] in ["Installed"]:
            print >> sys.stderr, \
            "WARNING Tool %s not in module_conf.xml, its status is %s" \
            % (tool[0], tool[1])
    return base

def module2softs(mod):
    """
        build list of softwares of one module
    """
    proc = subprocess.Popen('module help %s' % mod, shell=True, 
				stdin=subprocess.PIPE,
				stdout=subprocess.PIPE,
				stderr=subprocess.PIPE,
				)
    stdout_value, stderr_value = proc.communicate()
    helpmod = stderr_value.split('\n') #Pourquoi ca sort sur error?
    i = 0
    for item in helpmod:
        i += 1
        if item == 'package provides following commands:':
            break
    return [item[1:] for item in helpmod[i:] if item]


def build_programs_names(modules, toolid):
    """
        build list of programs with prog@package@version@command format
    """
    if modules.has_key(toolid):
        currentmodulesdict = {}
        for module in modules[toolid][0]:
            softslist = module2softs(module)
            for soft in softslist:
                currentmodulesdict[soft] = module
        try:    
            programs = ["prog@%s@%s" % ( \
                currentmodulesdict[command].replace('/', '@'), \
                command) for command in modules[toolid][1]]
        except KeyError:
            try:
                programs = ["prog@%s@%s" % ( \
                currentmodulesdict[command.split()[0]].replace('/', '@'), \
                command) for command in modules[toolid][1]]
            except KeyError:
                print >> sys.stderr, \
                "WARNING, Command %s no match a software in modules %s" % \
                (command, modules[toolid][0])
                programs = [command for command in modules[toolid][1]]

    return programs

def build_metadata(tools_list, module_dict):
    """
      builds general_dict
    """
    list_dict = []
    for tool in tools_list:
        base_modules_names = build_modules_names(tool, module_dict)
        gen_dict = {}
        #test if no module for this tool no build dictionnary
        if len(base_modules_names) != 0:
            metadata = json.loads(tool.metadata.decode("utf-8"))
            #pprint.pprint(metadata["tools"])

            for toolmeta in metadata["tools"]:
                #pprint.pprint(toolmeta)
                if toolmeta["guid"] == tool.tool_id:
                    progs = build_programs_names(module_dict, toolmeta["guid"])
                    gen_dict[u'_id'] = 'galaxy@%s@%s' % \
                        (base_modules_names[-1], toolmeta["id"])
                    gen_dict[u'description'] = toolmeta["description"]
                    gen_dict[u'name'] = toolmeta["name"]
            gen_dict[u'package'] = 'pack@%s' % base_modules_names[-1]
            #gen_dict[u'packages_uses'] = \
            #    ['pack@%s' % name for name in base_modules_names]
            gen_dict[u'programs'] = progs
            gen_dict[u'type'] = 'galaxy'
            gen_dict[u'url'] = \
                'https://galaxy.web.pasteur.fr/root?tool_id=%s' % tool.tool_id
            gen_dict[u'topic'] = []
            list_dict.append(gen_dict)
    return list_dict

def map_database(connection):
    """
        Database mapping
    """
    eng = create_engine(connection)
    metadata = MetaData()
    metadata.reflect(eng)
    base = automap_base(metadata=metadata)
    base.prepare()
    return base.classes, eng

def list_all_tools(datab, eng):
    """
        build a list of tools in galaxy
    """
    list_tools = []
    tool_version, tool_shed_repository = datab.tool_version, \
                                        datab.tool_shed_repository
    sele = select([tool_version.tool_id, tool_shed_repository.status, \
        tool_shed_repository.tool_shed, tool_shed_repository.owner, \
        tool_shed_repository.deleted, tool_shed_repository.uninstalled, \
        tool_shed_repository.metadata, tool_shed_repository.description, \
        tool_shed_repository.name]) \
        .where(tool_version.tool_shed_repository_id == tool_shed_repository.id)
    with eng.connect() as conn:
        result = conn.execute(sele)
        for row in result:
            list_tools.append(row)
    #pprint.pprint(list_tools)
    return list_tools

def groupby(users_stat):
    """
        function group by
    """
    stat_group = {}
    for row in users_stat:
        login = string.split(row[0], "@")[0]
        try:
            user_uid = getpwnam(login).pw_uid
            user_gname = getgrgid(getpwuid(user_uid).pw_gid).gr_name
            if user_gname in stat_group:
                stat_group[user_gname] = stat_group[user_gname] + row[1]
            else:
                stat_group[user_gname] = row[1]
        except KeyError:
            print "User %s doesn't exist in this system" % login
    return sorted(stat_group.items(), key=itemgetter(1))

def countjobs(datab, eng, date_in, date_out):
    """
        build a count list of tools using by user
    """
    jobs_stats_bytools = []
    jobs_stats_byusers = []
    job, galaxy_user = datab.job, datab.galaxy_user
#    sele= select([job.tool_id, galaxy_user.email, func.count(job.tool_id)]) \
#        .where(galaxy_user.id==job.user_id) \
#        .where(between(job.create_time, date_in, date_out)) \
#        .group_by(galaxy_user.email, job.tool_id) \
#        .order_by(galaxy_user.email, desc(func.count(job.tool_id)))

    sele = select([job.tool_id, func.count(job.tool_id)]) \
        .where(between(job.create_time, date_in, date_out)) \
        .group_by(job.tool_id) \
        .order_by(desc(func.count(job.tool_id)))

    sele2 = select([galaxy_user.email, func.count(job.user_id)]) \
            .where(galaxy_user.id == job.user_id) \
            .where(between(job.create_time, date_in, date_out)) \
            .group_by(galaxy_user.email) \
            .order_by(desc(func.count(job.user_id)))

    with eng.connect() as conn:
        result = conn.execute(sele)
        for row in result:
            jobs_stats_bytools.append(row)
    with eng.connect() as conn:
        result = conn.execute(sele2)
        for row in result:
            jobs_stats_byusers.append(row)

    bygroup = groupby(jobs_stats_byusers)

    return jobs_stats_bytools, jobs_stats_byusers, bygroup

def workflow_info(datab, eng):
    """
        Workflow informations recovery
    """
    workflows = {}
    workflow_step = datab.workflow_step
    sele = select([workflow_step.tool_id, workflow_step.workflow_id, \
        workflow_step.order_index]) \
        .order_by(workflow_step.workflow_id, workflow_step.order_index)
    with eng.connect() as conn:
        result = conn.execute(sele)
        for row in result:
            if workflows.has_key(row[1]):
                workflows[row[1]].append((row[0], row[2]))
            else:
                workflows[row[1]] = [(row[0], row[2])]
    inv_workflows = dict((tuple(v), k) for k, v in workflows.items())
    return inv_workflows

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
    #STAT_BYTOOLS, STAT_BYUSERS, STAT_BYGROUPS = countjobs(database, engine, args.date_in, args.date_out)
    #WORKFLOWS = workflow_info(database, engine)
    TOOLS_LIST = list_all_tools(database, engine)
    MODULE_DICT = build_xml_to_dict(args.module_file)
    BIOWEB_DICTS = build_metadata(TOOLS_LIST, MODULE_DICT)
    json_write(args.bioweb_json_file, BIOWEB_DICTS)

    
    #STAT_DIC = {"STAT_BYTOOLS" : [], "STAT_BYUSERS" : [], "STAT_BYGROUPS" : STAT_BYGROUPS}
    #for row in STAT_BYTOOLS:
    #    listrow = row.values()
    #    STAT_DIC["STAT_BYTOOLS"].append((listrow[0], listrow[1]))
    #    print "%s\t%d" % (listrow[0], listrow[1])
    #for row in STAT_BYUSERS:
    #    listrow = row.values()
    #    STAT_DIC["STAT_BYUSERS"].append((listrow[0], listrow[1]))
    #    print "%s\t%d" % (listrow[0], listrow[1])
    #for row in STAT_BYGROUPS:
    #    print "%s\t%d" % (row[0], row[1])
        
    #json_write(args.bioweb_json_file, STAT_DIC)
    
    #    listrow = row.values()
    #    if listrow[0] in tooldict:
    #       tooldict[listrow[0]][0].append([listrow[1], listrow[2]])
    #       print tooldict[listrow[0]]
    #       sorted(tooldict[listrow[0]][0], key=itemgetter(1))
    #       tooldict[listrow[0]][1] = tooldict[listrow[0]][1] + listrow[2]
    #    else:
    #        tooldict[listrow[0]] = [[[listrow[1], listrow[2]]], listrow[2]]
    #for l in tooldict:
    #    print l, tooldict[l]
    #for key, value in WORKFLOWS.items():
    #    print key, value


                                                                        
