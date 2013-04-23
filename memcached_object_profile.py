#!/bin/env python

# memcached_object_profile.py - profile objects stored in memcached
#
# Vito Laurenza <vitolaurenza@hotmail.com>, 2011
#
# requires:
# https://github.com/veetow/python-memcached-stats

import re
from optparse import OptionParser
from memcached_stats import MemcachedStats

def valueatpercentile(values, P):
    """ Given a list of numeric values, finds the value at a given percentile. """
    if not isinstance(values, (list)):
        raise ValueError, 'values must be a list.'
    if not isinstance(P, (int, float, long)) or not 0 <= P < 100:
        raise ValueError, 'Percentile P must be a numeric value greater than or equal to 0 and less than 100, ie: 0 <= P < 100'
    values.sort()
    N = len(values)
    R = P / 100.0 * N + 0.5
    if R <= 1:
        # return the first value in the list
        retval = values[0]
    elif R >= N:
        # return the last value in the list
        retval = values[N-1]
    elif float(int(R)) == R:
        # R is within the range and is a float representation of an integer
        # return the value at that position
        retval = values[int(R)-1]
    else:
        # R is within the range and is a float, so we have to interpolate
        k = int(R)
        Pk = 100.0 / N * (k - 0.5)
        retval = values[k-1] + N * (P - Pk) / 100 * (values[k] - values[k-1])
    return retval


def main():
    """ main """
    # parse options
    usage = 'Usage: %prog -h HOSTNAME [-p PORT] [[-r REGEX] [-r REGEX] ...] [-l NUM_OF_KEYS] [-v]'
    parser = OptionParser(usage, add_help_option=False, version="%prog v0.5")
    parser.add_option('-?', '--help', action='help')
    parser.add_option('-h', '--host', dest='hostname', metavar='HOSTNAME')
    parser.add_option('-p', '--port', dest='port', metavar='PORT', help='Default = 11211')
    parser.add_option('-l', '--limit', dest='limit', type='long', metavar='NUM_OF_KEYS',
                      help='Limit the number of matching keys to examine. Specify 0 for no limit. Default = 100')
    parser.add_option('-r', '--regex', action='append', dest='patterns', metavar='REGEX', 
                      help='Add one or more regex pattern(s) for filtering keys. Default = .*')
    parser.add_option('-v', '--verbose', action='store_true',  dest='verbose',
                      help='Be chatty.')
    (options, args) = parser.parse_args()

    # do some options housekeeping
    if not options.hostname:
        parser.print_help()
        parser.usage = None
        parser.error('HOSTNAME is required!')
    if options.port is None:
        options.port = 11211
    if options.limit is None:
        options.limit = 100
    if not options.patterns:
        options.patterns = [ur'.*']
    if options.verbose:
        print 'Loaded patterns: %s' % options.patterns

    # some inital values
    sizes = []
    rmb = re.compile(ur' b$')

    # compile our regex patterns
    for i, pattern in enumerate(options.patterns):
        options.patterns[i] = re.compile(pattern)

    if options.verbose:
        print 'Connecting to %s:%s' % (options.hostname, options.port)
    # connect to the given memcached instance
    mem = MemcachedStats(options.hostname, options.port)

    # get the total number of slabs
    total_slabs = len(mem.slab_ids())

    if options.verbose:
        print 'Collecting data for ALL keys...'
    # pull all the key details regardless of options.limit
    # because 'stats cachedump <slabid> <limit>' is on a per-slab basis
    details = mem.key_details(limit=0)
    
    if options.verbose:
        if options.limit == 0:
            print 'Looking for ALL keys matching the supplied regex patterns...'
        else:
            print 'Looking for at most %d keys matching the supplied regex patterns...' % options.limit
    for key in details:
        # tuple format: ('key_name', '\d+ b', '\d+ s')
        name, size, time = key
        for p in options.patterns:
            if p.search(name) is not None:
                # we found a match
                # remove the 'b' from the size in bytes and append it to the list
                sizes.append(float(rmb.sub('', size)))
                if options.verbose:
                    print key
        # stop looking if we've hit our limit on how many keys to examine
        if options.limit != 0 and len(sizes) == options.limit:
            break

    matches = len(sizes)
    total_size = sum(sizes)
    if options.verbose: print
    print 'Total number of slabs: %d' % total_slabs
    print 'Total number of matched objects in the cache: %d' % (matches)
    print 'Total size of all matched objects in the cache (in bytes): %d' % (total_size)
    if matches > 0:
        print 'Size statistics for the matched results (in bytes):'
        print '  Smallest:\t\t%d' % (min(sizes))
        print '  Largest:\t\t%d' % (max(sizes))
        print '  Average:\t\t%.2f' % (total_size/matches)
        print '  Mean:\t\t\t%.2f' % (valueatpercentile(sizes, 50))
        print '  90th percentile:\t%.2f' % (valueatpercentile(sizes, 90))
        print '  95th percentile:\t%.2f' % (valueatpercentile(sizes, 95))
        print '  99th percentile:\t%.2f' % (valueatpercentile(sizes, 99))


if __name__ == '__main__':
    main()
