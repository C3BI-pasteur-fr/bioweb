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
from sqlalchemy import create_engine, select, between, MetaData, func, desc
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
import string
import sys
import json

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

def build_metadata_two(tools_list, module_dict):
    """
      builds general_dict
    """
    list_dict = []
    for tool in tools_list:
        if module_dict.has_key(tool.tool_id):
            progs = module_dict[tool.tool_id][1]
        else:
            progs = [""]
        base_modules_names = build_modules_names(tool, module_dict)
        gen_dict = {}
        #test if no module for this tool no build dictionnary
        if len(base_modules_names) != 0:
            metadata = json.loads(tool.metadata.decode("utf-8"))
            for toolmeta in metadata["tools"]:
                print  tool.tool_id
                print toolmeta
                if toolmeta["guid"] == tool.tool_id:
                    gen_dict[u'_id'] = 'galaxy@%s@%s' % \
                        (base_modules_names[-1],  toolmeta["id"])
                    gen_dict[u'description'] = toolmeta["description"]
                    gen_dict[u'name']  = toolmeta["name"]
                    gen_dict[u'version'] = toolmeta["version"]
                    gen_dict[u'galaxy_id'] = toolmeta["guid"]
            gen_dict[u'package'] = ['pack@%s' % base_modules_names[-1]]
            gen_dict[u'packages_uses'] = \
                ['pack@%s' % name for name in base_modules_names]
            gen_dict[u'program'] = progs
            gen_dict[u'type'] = 'galaxy'
            gen_dict[u'url'] = \
                'https://galaxy.web.pasteur.fr/tool_runner?tool_id=%s' % tool.tool_id
            list_dict.append(gen_dict)
    return list_dict

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

def list_all_tools(database, engine):
    """
        build a list of tools in galaxy
    """
    list_tools = []
    tool_version, tool_shed_repository= database.tool_version, database.tool_shed_repository
    sele = select([tool_version.tool_id, tool_shed_repository.status, tool_shed_repository.tool_shed, tool_shed_repository.owner, tool_shed_repository.deleted, tool_shed_repository.uninstalled, tool_shed_repository.metadata, tool_shed_repository.description, tool_shed_repository.name]) \
        .where(tool_version.tool_shed_repository_id == tool_shed_repository.id)
    with engine.connect() as conn:
        result = conn.execute(sele)
        for row in result:
            list_tools.append(row)
    return list_tools

def jobs_count(database, engine, date_in, date_out):
    """
        build a count list of tools using by user
    """
    jobs_stats = []
    job, galaxy_user= database.job, database.galaxy_user
    sele= select([job.tool_id, galaxy_user.email, func.count(job.tool_id)]) \
        .where(galaxy_user.id==job.user_id) \
        .where(between(job.create_time, date_in, date_out)) \
        .group_by(galaxy_user.email, job.tool_id) \
        .order_by(galaxy_user.email, desc(func.count(job.tool_id)))
    with engine.connect() as conn:
        result = conn.execute(sele)
        for row in result:
            jobs_stats.append(row)
    return jobs_stats

def workflow_info(database, engine):
    """
        Workflow informations recovery
    """
    workflows = {}
    workflow_step = database.workflow_step
    sele = select([workflow_step.tool_id, workflow_step.workflow_id, workflow_step.order_index]) \
        .order_by(workflow_step.workflow_id, workflow_step.order_index)
    with engine.connect() as conn:
        result = conn.execute(sele)
        for row in result:
            if workflows.has_key(row[1]):
                workflows[row[1]].append((row[0],row[2]))
            else:
                workflows[row[1]] = [(row[0],row[2])]
    return workflows
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--universefile", help="config file of galaxy")
    parser.add_argument("--date_in", help="format is YYYY-MM-DD")
    parser.add_argument("--date_out", help="format is YYYY-MM-DD")
    parser.add_argument("--module_file", help="module_conf_file")
    parser.add_argument("--bioweb_json_file", help="json bioweb output file")
    
    
    args = parser.parse_args()
    config = ConfigParser.ConfigParser()
    config.read(args.universefile)
    database_connection = config.get('app:main', 'database_connection')
    database, engine = map_database(database_connection)
    STATISTIC_DB = jobs_count(database, engine, args.date_in, args.date_out)
    WORKFLOWS = workflow_info(database, engine)
    TOOLS_LIST = list_all_tools(database, engine)
    MODULE_DICT = build_xml_to_dict(args.module_file)
    BIOWEB_DICTS = build_metadata_two(TOOLS_LIST, MODULE_DICT)
    with open(args.bioweb_json_file, 'w') as bioweb_file:
        try:
            json.dump(BIOWEB_DICTS, bioweb_file, indent=4)
        except SystemExit:
            pass
    
    inv_WORKFLOWS = {tuple(v): k for k, v in WORKFLOWS.items()}
    
    for key, value in inv_WORKFLOWS.items():
        print key, value
    
