#!/bin/ksh
#
# The script invokes the dataGrooming java class to run some tests and generate a report and
#     potentially do some auto-deleteing.
#
# Here are the allowed Parameters.  Note - they are all optional and can be mixed and matched.
#
#  -f oldFileName  (see note below)
#  -autoFix 
#  -sleepMinutes nn
#  -edgesOnly
#  -skipEdges
#  -timeWindowMinutes nn
#  -dontFixOrphans
#  -maxFix
#  -skipHostCheck
#  -singleCommits
#  -dupeCheckOff
#  -dupeFixOn
#  -ghost2CheckOff
#  -ghost2FixOn
#
#
#
#
# NOTES:
# -f  The name of a previous report can optionally be passed in with the "-f" option. 
#     Just the filename --  ie. "dataGrooming.sh -f dataGrooming.201504272106.out"   
#     The file will be assumed to be in the directory that it was created in.
#     If a filename is passed, then the "deleteCandidate" vertex-id's and bad edges
#     listed inside that report file will be deleted on this run if they are encountered as
#     bad nodes/edges again.
#     
# -autoFix  If you don't use the "-f" option, you could choose to use "-autofix" which will
#           automatically run the script twice: once to look for problems, then after 
#           sleeping for a few minutes, it will re-run with the inital-run's output as
#           an input file.  
#
# -maxFix   When using autoFix, you might want to limit how many 'bad' records get fixed.
#           This is a safeguard against accidently deleting too many records automatically.
#           It has a default value set in AAIConstants:  AAI_GROOMING_DEFAULT_MAX_FIX = 15;
#           If there are more than maxFix candidates found -- then none will be deleted (ie. 
#           someone needs to look into it)
# 
# -sleepMinutes   When using autoFix, this defines how many minutes we sleep before the second run.
#           It has a default value set in AAIConstants:  AAI_GROOMING_DEFAULT_SLEEP_MINUTES = 7;
#           The reason we sleep at all between runs is that our DB is "eventually consistant", so
#           we want to give it time to resolve itself if possible.
#
# -edgesOnly    Can be used any time you want to limit this tool so it only looks at edges.
#            Note - as of 1710, we have not been seeing many purely bad edges, 
#            (ie. not associated with a phantom node) so this option is not used often.
#           
# -skipEdgeChecks  Use it to bypass checks for bad Edges (which are pretty rare).
#
# -timeWindowMinutes   Use it to limit the nodes looked at to ones whose update-timestamp tells us that it was last updated less than this many minutes ago.  Note this is usually used along with the skipEdgeChecks option.
#
# -dontFixOrphans   Since there can sometimes be a lot of orphan nodes, and they don't 
#           harm processing as much as phantom-nodes or bad-edges, it is useful to be
#           able to ignore them when fixing things.  
#
# -skipHostCheck    By default, the grooming tool will check to see that it is running
#           on the host that is the first one in the list found in:
#               aaiconfig.properties  aai.primary.filetransfer.serverlist
#           This is so that when run from the cron, it only runs on one machine.
#           This option lets you turn that checking off.
#
# -singleCommits    By default, the grooming tool will do all of its processing and then do
#           a commit of all the changes at once.  This option (maybe could have been named better)
#           is letting the user override the default behavior and do a commit for each
#           individual 'remove" one by one as they are encountered by the grooming logic. 
#           NOTE - this only applies when using either the "-f" or "-autoFix" options since 
#           those are the only two that make changes to the database.
#
# -dupeCheckOff    By default, we will check all of our nodes for duplicates.  This parameter lets
#           us turn this check off if we don't want to do it for some reason.
#
# -dupeFixOn    When we're fixing data, by default we will NOT fix duplicates  This parameter lets us turn 
#           that fixing ON when we are comfortable that it can pick the correct duplicate to preserve. 
#
# -ghost2CheckOff    By default, we will check for the "new" kind of ghost that we saw on
#           Production in early February 2016.  This parameter lets us turn this check off if we 
#           don't want to do it for some reason.
#
# -ghost2FixOn    When we're fixing data, by default we will NOT try to fix the "new" ghost nodes.  
#           This parameter lets us turn that fixing ON if we want to try to fix them. 
#
#
COMMON_ENV_PATH=$( cd "$(dirname "$0")" ; pwd -P )
. ${COMMON_ENV_PATH}/common_functions.sh

# TODO: There is a better way where you can pass in the function
# and then let the common functions check if the function exist and invoke it
# So this all can be templated out
start_date;
check_user;

processStat=$(ps -ef | grep '[D]ataGrooming');
if [ "$processStat" != "" ]
	then
	echo "Found dataGrooming is already running: " $processStat
	exit 1
fi

# Make sure that it's not already running
processStat=`ps -ef|grep aaiadmin|grep -E "org.onap.aai.dbgen.DataGrooming"|grep -v grep`
if [ "$processStat" != "" ]
	then
	echo "Found dataGrooming is already running: " $processStat
	exit 1
fi

source_profile;
execute_spring_jar org.onap.aai.datagrooming.DataGrooming $PROJECT_HOME/resources/logback.xml "$@"
end_date;
exit 0
