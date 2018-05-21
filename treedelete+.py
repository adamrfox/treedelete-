#!/usr/bin/python

import os
import sys
import getopt
import subprocess
import re
import time

def usage():
  sys.stderr.write ("Usage: treedelete+.py -r <path> -i <file> [-p #] [-j #] [-I <policy>] [-P #] [-v] [-T] [-w #] [-h]\n\n")
  sys.stderr.write ("       -r <path> | --root=<path> : Specifies a path to scan for directories\n")
  sys.stderr.write ("       -i <file> | --input=<file> : Specifies a file of paths to use or exclude\n")
  sys.stderr.write ("       -p # | --paths=# : Specifies the # of paths in each job (def: 1)\n")
  sys.stderr.write ("       -j # | --jobs=# : Specifies the # of jobs in the queue at once (def: 1)\n")
  sys.stderr.write ("       -I <policy> | --impact=<policy> : Specifies the impact policy of the jobs (def: MEDIUM)\n")
  sys.stderr.write ("       -P # | --priority=# : Specifies the priority of the jobs (def: 4)\n")
  sys.stderr.write ("       -v | --verbose : Turns on verbose mode (shows the commands (def: off\n")
  sys.stderr.write ("       -T | --TEST : Turns on test mode.  Only show commands, does not execute\n")
  sys.stderr.write ("       -w # | --waittime=# : Time in seconds to wait between queue checks (def: 30\n")
  sys.stderr.write ("       -h | --help : Prints Usage message\n")
  exit (0)

root = ''
infile = ''
concur_jobs = 1
concur_paths = 1
waittime = 30
path_list = []
path_list2 = []
TEST = False
impact = ""
priority = -1
VERBOSE = False
optlist, args = getopt.getopt (sys.argv[1:], 'r:i:j:p:w:hTI:P:v', ['root=', 'input=', 'jobs=', 'paths=', 'waittime=', 'help', 'TEST', 'impact=', 'priority=', 'verbose'])
for opt, a in optlist:
  if opt in ('-r', '--root'):
    root = a
  if opt in ('-i', '--input'):
    infile = a
  if opt in ('-j', '--jobs'):
    concur_jobs = int(a)
  if opt in ('-p', '--paths'):
    concur_paths = int(a)
  if opt in ('-w', '--wait'):
    waittime = int(a)
  if opt in ('-h', '--help'):
    usage()
  if opt in ('-T', '--TEST'):
    TEST = True
  if opt in ('-I', '--impact'):
    impact = a
  if opt in ('-P', '--priority'):
    priority = int(a)
  if opt in ('-v', '--verbose'):
    VERBOSE = True
if concur_jobs > 30:
  sys.stderr.write ("Concurrent jobs cannot be > 30\n")
  exit (1)

if root != "":
  ls = os.listdir (root)
  for f in ls:
    file = root + "/" + f
    if os.path.isdir(file):
      path_list.append (file)
if infile != "":
  with open (infile) as f:
    for ls in f:
      ls = ls.replace ('\n', '')
      if re.match(r'^-', ls):
        ls = ls[1:]
        if ls in path_list:
          path_list.remove(ls)
      elif os.path.isdir(ls):
        path_list.append (ls)
while path_list != []:
 # print path_list
  td_count = 0
  job_list = subprocess.Popen (['isi', 'job', 'ls'], stdout=subprocess.PIPE).communicate()[0]
  output = job_list.split ('\n')
  for l in output:
    if re.match(r'[0-9]+', l) != None:
      lf = l.split()
      if lf[1] == "TreeDelete":
        td_count += 1
  avail_jobs = 3-td_count
  td_jobs = min ([avail_jobs, concur_jobs])
  print "TreeDelete Jobs Running / Queued: " + str(td_count)
  print "Starting " + str(td_jobs) + " TreeDelete jobs"
  j = 0
  while j < td_jobs and path_list != []:
    cmd = ["isi", "job", "start", "TreeDelete"]
    if priority > -1:
      cmd.append("--priority="+str(priority))
    if impact != "":
      cmd.append("--impact="+impact)
    p = 0
    while p < concur_paths and path_list != []:
      cmd.append ('--paths=' + str(path_list.pop()))
      p += 1
    j += 1
    if TEST == True or VERBOSE == True:
      print ' '.join (cmd)
    if TEST == False:
      subprocess.call (cmd)
  if len(path_list) == 1:
    print str(len(path_list)) + " path still waiting"
  else:
    print str(len(path_list)) + " paths still waiting"
  if len(path_list) > 0:
    time.sleep (waittime)
print "Waiting for Queued jobs to finish..."
done = False
while done == False:
  td_count = 0
  job_list = subprocess.Popen (['isi', 'job', 'ls'], stdout=subprocess.PIPE).communicate()[0]
  output = job_list.split ('\n')
  for l in output:
    if re.match(r'[0-9]+', l) != None:
      lf = l.split()
      if lf[1] == "TreeDelete":
        td_count += 1
  if td_count == 0:
      done = True
  if td_count == 1:
    print str(td_count) + " job left"
  else:
    print str(td_count) + " jobs left"
  if td_count > 0:
    time.sleep (waittime)
print "Done"
    
