"""
Created on Jan. 30, 2015

@authors: Fabien Mareuil, Institut Pasteur, Paris
@contacts: fabien.mareuil@pasteur.fr
@project: bioweb_galaxy
@githuborganization: bioweb
"""
import ConfigParser
import argparse
import string
import sys
import json
import pprint
import subprocess
from sqlalchemy import create_engine, select, MetaData
from sqlalchemy.ext.automap import automap_base
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError



def export_to_Mongo(galaxy_dicts, connect=None):  
    """
        export bioweb dict into the db mongo
    """
    if not connect:  
        client = MongoClient()
    else:
        client = MongoClient(connect)
    dbmongo = client.bioweb
    catalog = dbmongo.catalog
    for doc in galaxy_dicts:
        try:
            catalog.insert(doc)
        except DuplicateKeyError:
            inserted_doc = catalog.find_one({'_id': doc['_id']})
            if inserted_doc['url'] == doc['url']:
                catalog.update({'_id': doc['_id']}, doc)
            else:
                print >> sys.stderr, \
                "WARNING Key %s already exist in the db but with a different url %s" % (doc['_id'], doc['url'])

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
        return a list of base names for _id, package, the last
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
    proc = subprocess.Popen('module help %s' % mod, shell=True, \
				        stdin=subprocess.PIPE, \
				        stdout=subprocess.PIPE, \
				        stderr=subprocess.PIPE, \
				        )
    stdout_value, stderr_value = proc.communicate()
    helpmod = stderr_value.split('\n') #Pourquoi ca sort sur error?
    i = 0
    for item in helpmod:
        i += 1
        if item == 'package provides following commands:':
            break
    return [item[1:] for item in helpmod[i:] if item]


def build_programs_ids(modules, toolid):
    """
        build list of programs with prog@package@version@command format
    """
    if modules.has_key(toolid):
        currentmodulesdict = {}
        for module in modules[toolid][0]:
            softslist = module2softs(module)
            for soft in softslist:
                currentmodulesdict[soft] = module
        sub_commands = []
        try:
            programs = ["prog@%s@%s" % ( \
                currentmodulesdict[command].replace('/', '@'), \
                command) for command in modules[toolid][1]]
            sub_commands = [build_sub_command(command, \
                currentmodulesdict[command].replace('/', '@'), \
                ) for command in modules[toolid][1]]
        except KeyError:
            try:
                programs = ["prog@%s@%s" % ( \
                currentmodulesdict[command.split()[0]].replace('/', '@'), \
                command.split()[0]) for command in modules[toolid][1]]
                sub_commands = [build_sub_command(command, \
                currentmodulesdict[command.split()[0]].replace('/', '@'), \
                ) for command in modules[toolid][1]]
            except KeyError:
                print >> sys.stderr, \
                "WARNING, Command %s no match a software in modules %s" % \
                (command, modules[toolid][0])
                programs = ["prog@%s" % command.split()[0] \
                            for command in modules[toolid][1]]
                sub_commands = [build_sub_command(command\
                ) for command in modules[toolid][1]]
    return programs, sub_commands

def build_sub_command(command, currentmodule=None):
    """
        build sub command id
    """
    if currentmodule:
        sub_command = "prog@%s" % currentmodule
    else:
        sub_command = "prog"
    for sub in command.split():
        sub_command = sub_command + "@%s" % sub
    return sub_command

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
                    progs, sub_commands = build_programs_ids(module_dict, \
                                        toolmeta["guid"])
                    gen_dict['_id'] = 'galaxy@%s@%s' % \
                        (base_modules_names[-1], toolmeta["id"])
                    gen_dict['description'] = toolmeta["description"]
                    gen_dict['name'] = toolmeta["name"]
            gen_dict['package'] = 'pack@%s' % base_modules_names[-1]
            #gen_dict[u'packages_uses'] = \
            #    ['pack@%s' % name for name in base_modules_names]
            gen_dict['programs'] = progs
            gen_dict['sub_commands'] = sub_commands
            gen_dict['type'] = 'galaxy'
            gen_dict['url'] = \
                'https://galaxy.web.pasteur.fr/root?tool_id=%s' % tool.tool_id
            gen_dict['topic'] = []
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
    parser.add_argument("--module_file", help="module_conf_file")
    parser.add_argument("--bioweb_json_file", help="json bioweb output file")
    args = parser.parse_args()


    database_connection = config_parsing(args.universefile)
    database, engine = map_database(database_connection)
    TOOLS_LIST = list_all_tools(database, engine)
    MODULE_DICT = build_xml_to_dict(args.module_file)
    BIOWEB_DICTS = build_metadata(TOOLS_LIST, MODULE_DICT)
    export_to_Mongo(BIOWEB_DICTS)
    #json_write(args.bioweb_json_file, BIOWEB_DICTS)
