import hashlib
import requests
import subprocess
import os
import sys


url = 'http://zk-web-prod/foursquare/config/hfileservice/sockets/' + sys.argv[1]
config = requests.get(url).json()

pairs = []

for v in config['collections']:
  col = v['collection']
  partition = v.get('partition', 0)
  hdfs = v['path'].replace('/hdfs/hadoop-alidoro-nn-vip', '')

  # copy the file to local
  local = hashlib.md5(hdfs).hexdigest()
  local = '/tmp/' + local

  if not os.path.exists(local):
    print "copying "+hdfs
    cmd = ['hadoop', 'fs', '-copyToLocal', hdfs, local]
    print " ".join(cmd)
    subprocess.check_call(cmd)

  pairs.append("%s/%s=%s"%(col, partition, local))

print "\n\n"
cmd = ['./thile'] + pairs

print cmd

if len(sys.argv) > 2:
  subprocess.check_call(cmd)
