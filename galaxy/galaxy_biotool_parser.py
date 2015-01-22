"""
Created on Oct. 23, 2014

@author: Olivia Doppelt-Azeroual, Institut Pasteur, Paris
@contact: olivia.doppelt@pasteur.fr
@project: toolinfowarehouse
@githuborganization: edamontology
"""

import sys
import os
import re
import pprint
import string
import argparse
import json

from bioblend.galaxy.client import ConnectionError
from bioblend.galaxy import GalaxyInstance


def build_tool_name(tool_id):
    """
    @tool_id: tool_id
    builds the tool_name regarding its toolshed id
   """
    print tool_id
    id_list = string.split(tool_id, '/')
    return string.join(id_list[-2:], '_')


def get_source_registry(tool_id):
    try:
    #    tool_id.index('toolshed')
        source = string.split(tool_id, '/')
        for i in range(len(source) - 1):
            if source[i].find('toolshed'):
                return (source[i + 1] + '_' + source[i - 1])
    except ValueError:
        print "ValueError:", tool_id
        return ""


def get_tool_name(tool_id):
    try:
        source = string.split(tool_id, '/')[-2]
        return source
    except ValueError:
        print "ValueError:", tool_id
        return ""

def build_xml_to_dict(module_conf_data):
    """
        return a dictionnary of module_conf.xml
    """
    import xml.etree.ElementTree as ET
    module_dict = {}
    tree = ET.parse(module_conf_data)
    root = tree.getroot()
    for child in root:
        module_dict[child.attrib['id']] = [string.split(child.attrib['module']), string.split(child.attrib['commands'])]
    return module_dict

def build_modules_names(tool,tools_dict):
    """
        return a list of base names for _id, package and packages_uses, the last
        in the list is the most important
    """
    base = []
    if tools_dict.has_key(tool[u'id']):
        for module in tools_dict[tool[u'id']][0]:
            name = module.replace("/","@")
            #bof mais j'ai pas mieux
            if module in ["ptools/0.99a", "ptools/0.99"]:
                [name] + base
            else:    
                base.append(name)
    else:
        print >> sys.stderr, "________WARNING________\n%s not in module_conf.xml" % tool[u'id']
    return base

def build_metadata_two(tools_meta_data, module_conf_data):
    """
      builds general_dict
      @param: tool_meta_data for one tool extracted from galaxy
    """
    tools_dict = build_xml_to_dict(module_conf_data)
    list_dict = []
    for tool in tools_meta_data:
        if tools_dict.has_key(tool[u'id']): 
            progs=tools_dict[tool[u'id']][1] 
        else:
            progs=[""]
        base_modules_names = build_modules_names(tool,tools_dict)
        gen_dict = {}
        gen_dict[u'description'] = tool[u'description']
        #test if no module for this tool no build dictionnary
        if len(base_modules_names) != 0:
            gen_dict[u'_id'] = 'galaxy@%s@%s' % (base_modules_names[-1], string.join(progs))
            gen_dict[u'name'] = tool[u'id']
            gen_dict[u'package'] = ['pack@%s' % base_modules_names[-1]]
            gen_dict[u'packages_uses'] = ['pack@%s' % name for name in base_modules_names]
            gen_dict[u'program'] = progs
            gen_dict[u'type'] = 'galaxy'
            gen_dict[u'url'] = 'https://galaxy.web.pasteur.fr/tool_runner?tool_id=%s' % tool[u'id']
            list_dict.append(gen_dict)
    return list_dict

def build_metadata_one(tool_meta_data, url):
    """
      builds general_dict
      @param: tool_meta_data for one tool extracted from galaxy
    """
    gen_dict = {k: tool_meta_data[k] for k in (u'version', u'description')}
    gen_dict[u'name'] = tool_meta_data[u'id'] #get_tool_name(tool_meta_data[u'id'])
    gen_dict[u'uses'] = [{"usesName": url,
                          "usesHomepage": url,
                          "usesVersion": gen_dict[u'version']
        }]
    gen_dict[u'collection'] = [url]
    gen_dict[u'sourceRegistry'] = get_source_registry(tool_meta_data[u'id'])
    gen_dict[u'softwareType'] = 'Tool'
    gen_dict[u'maturity'] = [{u'uri': "",
                            u'term': 'production'
                            }]
    gen_dict[u'platform'] = [{u'uri': "",
                              u'term': 'Linux'
                              }]
    # these fields need to be filled with MODULE ressource at Pasteur
    #gen_dict[u'language'] = []
    #gen_dict[u'topic'] = []
    #gen_dict[u'tag'] = []
    #gen_dict[u'licence'] = []
    #gen_dict[u'cost'] = []
    #gen_dict[u'credits'] = []
    #gen_dict[u'docs'] = []

    try:
        # citations are missing from the bioblend show tool
        # need adjustments to consider them once they are
        # included
        gen_dict[u'publications'] = [tool_meta_data[u"citations"]]
    except KeyError:
        pass
        #gen_dict[u'publications'] = []

    return gen_dict


def build_case_inputs(case_dict, input):
    dict_cases = {}
    for inp in input[u'cases']:

        for elem in inp[u'inputs']:
            if elem[u'type'] == u'data':
                if dict_cases.get(inp[u'value']) is None:
                    dict_cases[inp[u'value']] = [elem]
                else:
                    dict_cases[inp[u'value']].append(elem)

                # repeat in conditional

            if elem[u'type'] == u'repeat':
                try:
                    cases = elem[u'inputs'][0][u'cases']

                    for case in cases:
                        if case[u'inputs'] != []:
                            for case_input in case[u'inputs']:
                                if case_input[u'type'] == u'data':
                                    if dict_cases.get(inp[u'value']) is None:
                                        dict_cases[inp[u'value']] = [case_input]
                                    else:
                                        dict_cases[inp[u'value']].append(case_input)

                except KeyError:
#                    print "KeyError key == REPEAT"
                    for el in elem[u'inputs']:
                        if el[u'type'] == u'data':
                            if dict_cases.get(inp[u'value']) is None:
                                dict_cases[inp[u'value']] = [el]
                            else:
                                dict_cases[inp[u'value']].append(el)

    case_dict.update({i: j for i, j in dict_cases.items() if len(j) != 0})


def build_input_for_json(list_inputs):
    liste = []
    inputs = {}
    try:
        try:

            for input in list_inputs:
                inputDict = {}
                inputDict[u'dataType'] = {u'uri': "", u'term': input[u'type']}

                try:
                    formatList = string.split(input[u'format'], ',')
                except AttributeError:
                    formatList = ["AnyFormat"]

                list_format = []
                for format in formatList:
                    dict_format = {u'uri': "", u'term': format}
                    list_format.append(dict_format)
                inputDict[u'dataFormat'] = list_format
                inputDict[u'dataHandle'] = input[u'label']
                liste.append(inputDict)

        except KeyError:
                inputDict[u'dataType'] = {u'uri': "", u'term': input[u'type']}
                formatList = input[u'extensions']
                for format in formatList:
                    inputDict[u'dataFormat'].append({u'uri': "", u'term': format})
                inputDict[u'dataHandle'] = input[u'label']
                liste.append(inputDict)

    except KeyError:
        inputDict[u'dataType'] = {u'uri': "", u'term': input[u'type']}
        inputDict[u'dataFormat'] = []
        inputDict[u'dataHandle'] = input[u'label']
        liste.append(inputDict)

    return liste


def build_fonction_dict(tool_meta_data):
    """
    builds function dict
    2 steps for inputs, get only the data format and
    dict comprehension to keep only important info
    1 steps for outputs, only dict comprehension
    """
    func_dict = {}
    func_list = []
    inputs = {}
    outputs = []
    inputs_fix = []
    dict_cases = {}
    inputs_case = {}

    for input in tool_meta_data[u'inputs']:
        if input[u'type'] == u'data':
            inputs_fix.append(input)
        # repeat not in conditional
        if input[u'type'] == u'repeat':
            for rep in input[u'inputs']:
                if rep[u'type'] == u'data':
                    print 'repeeeeeeaaaaaatttt'
                    inputs_fix.append(rep)
                elif rep[u'type'] == "conditional":
                    build_case_inputs(dict_cases, rep)
        if input[u'type'] == "conditional":
            build_case_inputs(dict_cases, input)


#__________________INPUT DICT _________________________
    if len(dict_cases) == 0:
        inputs["input_fix"] = build_input_for_json(inputs_fix)
    else:
        for key, case in dict_cases.iteritems():
            inputs[key] = build_input_for_json(case) + build_input_for_json(inputs_fix)


#_____________OUTPUT DICT_______________________________________

    for output in tool_meta_data[u'outputs']:
        outputDict = {}
        outputDict[u'dataType'] = []
        outputDict[u'dataFormat'] = {u'uri': "", u'term': output[u'format']}
        outputDict[u'dataHandle'] = output[u'name']
        outputs.append(outputDict)

    if inputs.get("input_fix") is None:
        for input_case_name, item in inputs.items():
            func_dict = {}
            func_dict[u'description'] = tool_meta_data[u'description']
            func_dict[u'functionName'] = []
            func_dict[u'output'] = outputs
            func_dict[u'input'] = item
            func_dict[u'functionHandle'] = 'MainFunction'
            func_dict[u'annot'] = input_case_name
            func_list.append(func_dict)
    else:
        func_dict[u'description'] = tool_meta_data[u'description']
        func_dict[u'functionName'] = []
        func_dict[u'output'] = outputs
        func_dict[u'input'] = inputs[u"input_fix"]
        func_dict[u'functionHandle'] = 'MainFunction'
        func_list.append(func_dict)
#        print("TYPE FUNC DICT:", type(func_dict))

    return func_list


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Galaxy instance tool\
        parsing, for integration in biotools/bioregistry")

    parser.add_argument("--galaxy_url", help="url to the analyzed galaxy instance")
    parser.add_argument("--api_key", help="galaxy user api key")
    parser.add_argument("--tool_dir", help="directory to store the tool\
        json (needs to be created before running the script")
    parser.add_argument("--modulegalaxy_file", help="module_conf.xml file")
    parser.add_argument("--bioweb_json_file", help="json bioweb output file")
    parser.add_argument("--collection_name", help="collection name matchine the galaxy url")

    args = parser.parse_args()
    gi = GalaxyInstance(args.galaxy_url, key=args.api_key)

    tools = gi.tools.get_tools()
    tools_meta_data = []
    new_dict = {}
    json_ext = '.json'

    for i in tools:
        try:
            # improve this part, important to be able to get all tool from any toolshed
            if not i['id'].find("galaxy.web.pasteur.fr") or not i['id'].find("testtoolshed.g2.bx.psu.edu") or not i['id'].find("toolshed.g2.bx.psu.edu"):
                tool_metadata = gi.tools.show_tool(tool_id=i['id'], io_details=True, link_details=True)
                tools_meta_data.append(tool_metadata)
          #  else:
           #     print i['id']
        except ConnectionError:
            print "ConnectionError"
            pass
    
    if args.modulegalaxy_file:
        bioweb_dicts = build_metadata_two(tools_meta_data, args.modulegalaxy_file)
        with open(args.bioweb_json_file,'w') as bioweb_file:
            try:
                json.dump(bioweb_dicts, bioweb_file, indent=4)
            except SystemExit:
                    pass
    
    if args.tool_dir:
        for tool in tools_meta_data:
            tool_name = build_tool_name(tool[u'id'])
            try:

                function = build_fonction_dict(tool)
                #print "TYPE FUNCTION:", type(function)
                if len(function) > 1:
                    print "THERE WILL BE  " + str(len(function)) + "json"
                    for func in function:
                        pprint.pprint(func)
                        #inputs = func[u"input"]
                        name = re.sub("[\.\,\:;\(\)\./]", "_", func[u'annot'], 0, 0)
                        with open(os.path.join(os.getcwd(), args.tool_dir, tool_name + "_"+ name + json_ext), 'w') as tool_file:
                            general_dict = build_metadata_one(tool, args.galaxy_url)
                            general_dict["function"] = func
                            json.dump(general_dict, tool_file, indent=4)
                            tool_file.close()
 
                else:
                    with open(os.path.join(os.getcwd(), args.tool_dir, tool_name + json_ext), 'w') as tool_file:
                        general_dict = build_metadata_one(tool, args.galaxy_url)
                        general_dict["function"] = function[0]
                        json.dump(general_dict, tool_file, indent=4)
                        tool_file.close()


            except SystemExit:
                pass
