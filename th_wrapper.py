#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 lookcrabs <6787558+lookcrabs@users.noreply.github.com>
#
# Distributed under terms of the MIT license.
#
# FROM: https://github.com/lookcrabs/TruffleHog_Scripts/blob/master/th_wrapper.py

"""
This is a wrapper around truffleHog to add whitelist/exclusions so I can
easily incluse this + a small json file to alert on pws or high entropy
commits.
"""

import argparse
import json
import git
import hashlib
import logging
import os
import re
import sys
import tempfile
import truffleHog
from truffleHog import truffleHog as TH ##eventually use as module


FALSE = [ 'FALSE', 'False', 'false', False, 0 ]
TRUE =  [ 'TRUE',  'True',  'true',  True,  1 ]
NO =    [ 'NO',    'No',    'no',    'n',  'N',  False,  0 ]
YES =   [ 'YES',   'Yes',   'yes',   'y',  'Y',  True,   1 ]
YNO = YES + NO

DEFAULT_WHITELIST = []
DEFAULT_CONFIG = {
  "bannerConfig": {
      "lineLength": 80,
      "bannerChar": "!",
      "bannerKeys": [
        "branch",
        "commitHash",
        "path",
        "diff",
        "reason",
        "stringsFound"
      ]
  },
  "debug": False,
  "flags": {
      "do_regex": True,
      "do_entropy": True,
      "max_depth": 50
    },
  "gitRemote": None,
  "whitelistF": "truffles.json",
  "configF": "thog_config.json",
  "hardExcludes": [
                    "th_wrapper.py",
                    "thog_config.json",
                    "truffles.json"
                  ],
  "regexRules": {
    "Slack Token": "(xox[p|b|o|a]-[0-9]{12}-[0-9]{12}-[0-9]{12}-[a-z0-9]{32})",
    "RSA private key": "-----BEGIN RSA PRIVATE KEY-----",
    "SSH (OPENSSH) private key": "-----BEGIN OPENSSH PRIVATE KEY-----",
    "SSH (DSA) private key": "-----BEGIN DSA PRIVATE KEY-----",
    "SSH (EC) private key": "-----BEGIN EC PRIVATE KEY-----",
    "PGP private key block": "-----BEGIN PGP PRIVATE KEY BLOCK-----",
    "Facebook Oauth": "[f|F][a|A][c|C][e|E][b|B][o|O][o|O][k|K].*['|\"][0-9a-f]{32}['|\"]",
    "Twitter Oauth": "[t|T][w|W][i|I][t|T][t|T][e|E][r|R].*['|\"][0-9a-zA-Z]{35,44}['|\"]",
    "GitHub": "[g|G][i|I][t|T][h|H][u|U][b|B].*[['|\"]0-9a-zA-Z]{35,40}['|\"]",
    "Google Oauth": "(\"client_secret\":\"[a-zA-Z0-9-_]{24}\")",
    "AWS API Key": "AKIA[0-9A-Z]{16}",
    "Heroku API Key": "[h|H][e|E][r|R][o|O][k|K][u|U].*[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}",
    "Generic Secret": "[s|S][e|E][c|C][r|R][e|E][t|T].*['|\"][0-9a-zA-Z]{32,45}['|\"]",
    "Generic API Key": "[a|A][p|P][i|I][_]?[k|K][e|E][y|Y].*['|\"][0-9a-zA-Z]{32,45}['|\"]",
    "Slack Webhook": "https://hooks.slack.com/services/T[a-zA-Z0-9_]{8}/B[a-zA-Z0-9_]{8}/[a-zA-Z0-9_]{24}",
    "Generic Password": "[P|p][A|a][S|s][S|s].*="
  }
}



class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

## Message Functions Start ##
def printDebug(conf, message):
    if conf['debug']:
        message_template = "{}DEBUG::{} {}".format(bcolors.WARNING,
                                                   bcolors.ENDC,
                                                   message)
        logging.debug(message_template)

def printKeys(conf, match):
    bannerConfig = conf['bannerConfig']
    bannerKeys = bannerConfig['bannerKeys']
    dictSep = "{}{}{}".format(bcolors.WARNING, '-' * 20,  bcolors.ENDC)
    if isinstance(match, dict):
        for k,v in match.items():
            if k in bannerKeys:
                keyString = "{}{}{}".format(bcolors.OKGREEN, k, bcolors.ENDC)
                logging.info("{}: {}".format(keyString, v))
        logging.info(dictSep)

def printBanner(conf, bannerMessage):
    ## Make a banner of equal size using a uniform character
    bannerConfig = conf['bannerConfig']
    lineLength = bannerConfig['lineLength']
    bannerChar = bannerConfig['bannerChar']
    lineSpacer = int(((lineLength - len(bannerMessage) - 2) /2))
    bannerSides = bcolors.FAIL + bannerChar + bcolors.ENDC

    logging.info(bannerSides * lineLength)
    logging.info("{}{}{}{}{}".format(bannerSides,
                              " " * lineSpacer,
                              bannerMessage,
                              " " * lineSpacer,
                              bannerSides))
    logging.info(bannerSides * lineLength)

def promptAnswer(conf, question_string):
    #Prompt user for question and store answer as boolean
    answer = ''
    while answer not in YNO:
        if sys.version_info[0] >= 3:
            answer = input("{} [y/N]  ".format(question_string))
        else:
            answer = raw_input("{} [y/N] ".format(question_string))
        if answer in YES:
            printDebug(conf, "Answered {}".format(answer))
            return True
        elif answer in NO:
            printDebug(conf, "Answered {}".format(answer))
            return False
        elif answer == '':
            printDebug(conf, "Answered {}".format(answer))
            return False
        else:
            print("Please type 'Y' for yes or 'N' for no.")
            answer = ''

## Message Functions End ##

def buildkwargs(**kwargs):
    return kwargs

#def gitRoot(path):
#        git_repo = git.Repo(path, search_parent_directories=True)
#        git_root = git_repo.git.rev_parse("--show-toplevel")
#        return(git_root)

def clone_git_repo_override(git_url):
    if 'PWD' in os.environ:
        github_base_path = os.environ['PWD']
    else:
        github_base_path = dir_path = os.path.dirname(os.path.realpath(__file__))

    project_path = tempfile.mkdtemp(dir=github_base_path, prefix="TruffleTmp")
    git.Repo.clone_from(git_url, project_path)
    return project_path

def getMatches(conf, path):
    #Run TruffleHog against github repo and store results in list
    matches = []
    ## lets build our options we're going to pass to truffleHog
    ## This should probably go in it's own function.
    kwargs = dict()
    if 'regexRules' in conf:
        if conf['regexRules'] and conf['regexRules'] is not None:
            customRegexes = conf['regexRules']
            for rule in customRegexes:
                customRegexes[rule] = re.compile(customRegexes[rule])
            kwargs = buildkwargs(git_url = path,
                    custom_regexes = customRegexes,
                    **conf['flags'])
        else:
            kwargs = buildkwargs(git_url = path,
                    **conf['flags'] )
    if 'sincecommit' in conf:
        if conf['sincecommit'] and conf['sincecommit'] is not None:
            sinceCommit = conf['sincecommit']
            kwargs = buildkwargs(since_commit = sinceCommit,
                                 **kwargs)

    printDebug(conf, "Calling TruffleHog with the following options\n {}".format(
                kwargs))
    output = TH.find_strings(**kwargs)
    printDebug(conf, "found issues: {}".format(len(output['foundIssues'])))
    for i in range(len(output['foundIssues'])):
        issueFile = output['foundIssues'][i]
        issue = json.load(open(issueFile))
        issue_json= issueJson(issue)
        if issue_json not in matches:
            matches.append(issue_json)
    printDebug(conf, "Found {} Matches".format(len(matches)))
    return matches

def writeDB(conf, whitelist):
    ## Overwrite existing whitelist file with new whitelist
    filePath = conf['whitelistF']
    absPath = os.path.abspath(filePath)
    with open(absPath, 'w') as f:
        json.dump(whitelist, f, indent=4, sort_keys=True)
    return whitelist

def readDB(conf):
    filepath = conf['whitelistF']
    absPath = os.path.abspath(filepath)
    with open(absPath, 'r') as f:
        whitelist = json.load(f)
    return whitelist

def addMatch(whitelist, match):
    if match not in whitelist:
        whitelist.append(match)
    return whitelist

def issueJson(match):
    found_issues = match
    found_issue_dict = {}
    found_issue_dict['diff'] = "{}".format(hashlib.sha256(found_issues['diff'].encode('utf-8')).hexdigest())
    found_issue_dict['path'] = "{}".format(found_issues['path'])
    found_issue_dict['branch'] = "{}".format(found_issues['branch'])
    found_issue_dict['commit'] = "{}".format(hashlib.sha256(found_issues['commit'].encode('utf-8')).hexdigest())
    found_issue_dict['stringsFound'] = [ x for x in found_issues['stringsFound'] ]
    found_issue_dict['printDiff'] = "{}".format(hashlib.sha256(found_issues['printDiff'].encode('utf-8')).hexdigest())
    found_issue_dict['commitHash'] = "{}".format(found_issues['commitHash'])
    found_issue_dict['reason'] = "{}".format(found_issues['reason'])
    issue_json = json.dumps(found_issue_dict)
    return issue_json



def parseMatches(conf, whitelist, matches):
    ## Check matches against whitelist and return a list of missing Issues
    mismatches = []
    printDebug(conf, "{} Matches, {} Whitelisted".format(len(matches), len(whitelist)))
    for match in matches:
        match = json.loads(match)
        printDebug(conf, 'parsing {} match'.format(match))
        if match not in whitelist:
            matches_path = 0
            for excludePath in conf['hardExcludes']:
                printDebug(conf, "type of match['path'] == {}".format(type(match['path'])))
                printDebug(conf, "type of excludePath == {}".format(type(excludePath)))
                printDebug(conf, "checking if {} in {}".format(match['path'], excludePath))
                if excludePath in match['path']:
                    matches_path += 1
                printDebug(conf, "\"{}\" not in \"{}\"".format(excludePath, str(match['path'])))
            if matches_path < 1:
                printDebug(conf, "Match not in whitelist: {}".format(json.dumps(match, indent=4, sort_keys = True)))
                mismatches.append(match)
    printDebug(conf, "found {} missing git diffs".format(len(mismatches)))
    printDebug(conf, "Missing: {}, Whitelist: {}, Total: {}".format(
                                    len(mismatches),
                                    len(whitelist),
                                    len(mismatches) + len(whitelist)))
    return mismatches

def parseMismatches(conf, whitelist, mismatches):
    try:
        for mi in range(len(mismatches)):
            match = mismatches[mi]
            MessageCount = "Whitelist: {}, MatchNum: {}, Mismatches: {}".format(
                    len(whitelist),
                    mi + 1,
                    len(mismatches))
            printDebug(conf, MessageCount)
            printKeys(conf, match)
            answer = promptAnswer(conf, "Should we exclude this match?")
            if answer in TRUE:
                printDebug(conf, "Answered {}. Adding to whitelist".format(answer))
                whitelist = addMatch(whitelist, match)
                continue
            elif answer in FALSE:
                printDebug(conf, "Answered {}. Skipping secret".format(answer))
                continue
        writeDB(conf, whitelist)
    except KeyboardInterrupt as e:
        writeDB(conf, whitelist)

def errorExceptions(conf, mismatches):
    bannerMessage = "Exceptions Found"
    bannerKeys = conf['bannerConfig']['bannerKeys']
    printBanner(conf, bannerMessage)
    printBanner(conf, "{} Mismatches Found".format(len(mismatches)))
    for mismatch in mismatches:
        for k,v in mismatch.items():
            if k in bannerKeys:
                logging.error("{}{}{} : {}".format(bcolors.OKGREEN, k, bcolors.ENDC, v))
        logging.error(bcolors.WARNING + "-------------------------" + bcolors.ENDC)
    printBanner(conf, bannerMessage)
    if conf['debug']:
        printBanner(conf, "Summary: {} Issues".format(len(mismatches)))

    raise RuntimeError("Secrets found outside of Whitelist")

truffleHog.truffleHog.clone_git_repo = clone_git_repo_override


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wrapper around Trufflehog for exclusions")
    parser.add_argument("-w",
                        "--whitelist",
                        default="truffles.json",
                        help="path to the whitelist file")
    parser.add_argument("-c",
                        "--config",
                        default="thog_config.json",
                        help="""path to the config file
                                if the file in the local directory
                                it is automatically imported""")
    parser.add_argument("-g",
                        "--gitRemote",
                        default=None,
                        type=str,
                        help="github url to pull")
    parser.add_argument("-s",
                        "--sincecommit",
                        default=None,
                        type=str,
                        help="Start scanning from this commit forward\n\
                              IE: bb08c4fc6764d8fffe17507029611fec72493192")
    parser.add_argument("-i",
                        "--init",
                        action="store_true",
                        help="add all exceptions to the whitelist and exit")
    parser.add_argument('--check',
                        action="store_true",
                        help="Whether to check/append the whitelist or error")
    parser.add_argument('--cinit',
                        action="store_true",
                        help="Create default Thog Config and Exit")
    parser.add_argument('--debug',
                        action="store_true",
                        help="Print Debug output")
    args = parser.parse_args()

    #first load default config and whitelist
    conf=DEFAULT_CONFIG
    whitelist = DEFAULT_WHITELIST
    # if config file is specified
    if args.config and args.config is not None:
        printDebug(conf, "config argument passed. Parsing config file")
        ## Try to open it and merge over the default config
        config_filepath = os.path.abspath(args.config)
        config_filename = config_filepath.split('/')[-1]
        ## If the file exists try to merge over the default config
        if os.path.exists(config_filepath):
            with open(config_filepath, 'r') as f:
                conf_x = json.load(f)
                for k,v in conf_x.items():
                    if k in conf:
                        conf[k] = v
        ## overwrite the default with config_filepath so we know where this is.
        conf['configF'] = config_filepath
        ## add the filename to hardExcludes so we skip any matches to this bog
        if config_filename not in conf['hardExcludes']:
                conf['hardExcludes'].append(config_filename)
        ## add the filepath too
        if config_filepath not in conf['hardExcludes']:
                conf['hardExcludes'].append(config_filepath)
    ## now lets merge any flags passed over that starting whitelist_path
    if args.whitelist and args.whitelist is not None:
        printDebug(conf, "Whitelist Argument passed. Parsing whitelist")
        whitelist_filepath = os.path.abspath(args.whitelist)
        whitelist_filename = whitelist_filepath.split('/')[-1]
        ## overwrite the default whitelist path so we know where it is.
        conf['whitelistF'] = whitelist_filepath
        ## add whitelist to hard_excludes as well
        if whitelist_filename not in conf['hardExcludes']:
            conf['hardExcludes'].append(whitelist_filename)
        ## add full path just in case
        if whitelist_filepath not in conf['hardExcludes']:
            conf['hardExcludes'].append(whitelist_filepath)
        ## lets just assume it exists and overwrite any defaults
        if os.path.exists(whitelist_filepath):
            with open(whitelist_filepath, 'r') as f:
                whitelist_x = json.load(f)
                printDebug(conf, "{} items in whitelist before parsing".format(len(whitelist)))
                for k in whitelist_x:
                    printDebug(conf, "{} items in whitelist before addmatch".format(len(whitelist)))
                    whitelist = addMatch(whitelist, k)
                    printDebug(conf, "{} items in whitelist after addmatch".format(len(whitelist)))
                printDebug(conf, "{} Items in whitelist after parsing".format(len(whitelist)))
    if args.gitRemote and args.gitRemote is not None:
        conf['gitRemote'] = args.gitRemote
    if args.sincecommit and args.sincecommit is not None:
        conf['sincecommit'] = args.sincecommit
    if args.debug and args.debug is not None:
        conf['debug'] = args.debug
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    #Lastly before we do anything lets dump the config for debug
    printDebug(conf, "{}".format(json.dumps(conf,
                                            indent=4,
                                            sort_keys=True)))
    ## That should be a complete config now. Lets check the flags that just exit
    if args.cinit and args.cinit is not None:
        #if cinit is passed just write the current config and exit
        with open(conf['configF'], 'w') as f:
            json.dump(conf, f, indent=4, sort_keys=True)
            sys.exit(0)
    ## for everything else we need to grab a list of matches so lets do that
    if conf['gitRemote'] and conf['gitRemote'] is not None:
        remote_matches = getMatches(conf, conf['gitRemote'])
    else:
        remote_matches = []
    all_matches = remote_matches
    mismatches = parseMatches(conf, whitelist, all_matches)
    ##  if init is passed add all matches to whitelist
    ##+ write the whitelist to the whitelist location
    ##+ exit
    if args.init and args.init is not None:
        for match in mismatches:
            whitelist = addMatch(whitelist, match)
        writeDB(conf, whitelist)
        sys.exit(0)
    ## if the check flag is set we need to error on the new findings
    if args.check and args.check is not None:
        if mismatches and len(mismatches) > 0:
            errorExceptions(conf, mismatches)
    else:
    ##  If check isn't passed we should walk the matches and
    ##+ Add them to the exceptions list
        parseMismatches(conf, whitelist, mismatches)
        sys.exit(0)
