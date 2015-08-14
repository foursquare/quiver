#!/usr/bin/env python2.7

import argparse
import hashlib
import os
import requests
import subprocess
import sys


def lookup_collections(socket):
  url = 'http://zk-web-prod/foursquare/config/hfileservice/sockets/' + socket
  config = requests.get(url).json()

  collections = {}

  for v in config['collections']:
    col = v['collection']
    partition = v.get('partition', 0)
    hdfs = v['path'].replace('/hdfs/hadoop-alidoro-nn-vip', '')
    collections['%s/%s'%(col, partition)] = hdfs

  return collections


def download(collections, output, verbose):
  downloaded = {}

  print "Fetching files..."

  for name, hdfs in collections.iteritems():
    # copy the file to local
    local = hashlib.md5(hdfs).hexdigest()
    local = os.path.join(output, local)

    if os.path.exists(local):
      print "\t%s already exists locally."%name
      if verbose:
        print "\t\t"+local
    else:
      print "\tDownloading %s..."%name
      cmd = ['hadoop', 'fs', '-copyToLocal', hdfs, local]
      if args.verbose:
        print "\t "+(" ".join(cmd))
      subprocess.check_call(cmd)
    downloaded[name] = local

  return downloaded


def run(args):
  print "Looking up config for %s"%args.config
  collections = lookup_collections(args.config)
  print "Configured to serve:\n\t%s\n"%("\n\t".join(collections.keys()))
  downloaded = download(collections, args.local, args.verbose)
  pairs = ["%s=%s"%(name, local) for name, local in downloaded.iteritems()]

  mlock = []
  if args.lock:
    mlock = ['--mlock']

  cmd = [args.binary] + mlock + pairs

  if args.verbose:
    print " ".join(cmd)

  print "\n"

  if args.run:
    try:
      print "Starting %s"%args.binary
      subprocess.check_call(cmd)
    except KeyboardInterrupt:
      pass
  else:
    print "DRY RUN. To start server, re-run with --run."


if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument("--verbose", "-v", action="store_true", help="verbose")
  parser.add_argument("--run", action="store_true", help="actually run it")
  parser.add_argument("--lock", action="store_true", help="mlock")
  parser.add_argument("--binary", default="./thile", help="path to server")
  parser.add_argument("--local", default='/tmp', help="where to write local files")
  parser.add_argument("config", help="'socket' (host/port) to read configs for")
  args = parser.parse_args()

  run(args)

