#!/usr/bin/env python

# This script is typically used by Mesos committers to push a review chain to
# ASF git repo and mark the reviews as submitted on ASF ReviewBoard.
#
# TOD)(vinod): Also post the commit message to the corresponding ASF JIRA
# tickets and resolve them.

import argparse
import os
import re

from subprocess import check_output

REVIEWBOARD_URL =  'https://reviews.apache.org'

parser = argparse.ArgumentParser()

parser.add_argument('-n',
                    '--dry-run',
                    action='store_true',
                    help='Perform a dry run.')

parser.add_argument('-t',
                    '--tracking-branch',
                    help='Tracking branch',
                    required=True)

parser.add_argument('-r',
                    '--remote',
                    help='Remote repository name')

args = parser.parse_args()

tracking_branch = args.tracking_branch
tracking_branch_ref = check_output([
                                   'git',
                                   'show-ref',
                                   '--hash',
                                   tracking_branch]).strip()

# Figure out the remote repo to push the commits to based on the command line
# argument or tracking branch.
remote = args.remote or tracking_branch.split('/')[0] or "origin/master"

print 'Remote repo %s' % remote

current_branch_ref = check_output(['git', 'symbolic-ref', 'HEAD']).strip()
current_branch = current_branch_ref.replace('refs/heads/', '', 1)


merge_base = check_output([
                          'git',
			  'merge-base',
			  tracking_branch,
			  current_branch_ref
			  ]).strip()

if merge_base != tracking_branch_ref:
    print 'Please rebase your current branch on top of %s' % tracking_branch
    sys.exit(1)

log = check_output([
                   'git',
                   '--no-pager',
                   'log',
                   '--no-color',
                   '--reverse',
                    merge_base + '..HEAD']).strip()

if len(log) <= 0:
    print 'No new changes compared with %s branch!' % tracking_branch
    sys.exit(1)


review_ids = []
for line in log.split('\n'):
    pos = line.find('Review: ')
    if pos != -1:
        pattern = re.compile('Review: ({url})$'.format(
            url=os.path.join(REVIEWBOARD_URL, 'r', '[0-9]+')))
        match = pattern.search(line.strip().strip('/'))
        if match is None:
            print "\nInvalid ReviewBoard URL: '{}'".format(line[pos:])
            sys.exit(1)

        url = match.group(1)
        review_ids.append(os.path.basename(url))


print 'Found reviews: ', review_ids

# Push the current branch upstream.
if not args.dry_run:
    check_output([
                 'git',
                 'push',
                 remote,
                 '%s:master' % current_branch
                 ]);

# Close the reviews on ReviewBoard.
for review_id in review_ids:
    print 'Closing review ', review_id
    if not args.dry_run:
        check_output(['rbt', 'close', review_id])
