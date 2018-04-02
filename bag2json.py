#!/usr/bin/env python
# encoding: utf-8

import argparse
import json

import rosbag_core


def buildParser():
    """
    Builds the parser for reading the command line arguments
    """

    parser = argparse.ArgumentParser(
        description='Script to parse bagfile to json file')
    parser.add_argument('-b', '--bag', help='Bag file to read',
                        required=True, type=str)
    parser.add_argument('-i', '--include',
                        help='list or regex for topics to include',
                        required=False, nargs='*')
    parser.add_argument('-e', '--exclude',
                        help='list or regex for topics to exclude',
                        required=False, nargs='*')
    parser.add_argument('-o', '--output',
                        help='name of the output file',
                        required=True, nargs='*')
    return parser


def do_work(bag, include, exclude, output):
    # covert a lenght one value to a regex
    if include is not None and len(include) == 1:
        include = include[0]

    # covert a lenght one value to a regex
    if exclude is not None and len(exclude) == 1:
        exclude = exclude[0]

    with open(output, 'w') as outfile:
        json.dump(rosbag_core.bag_to_json(bag, include, exclude), outfile)


if __name__ == '__main__':
    ''' Main entry point for the function. Reads the command line arguements
    and performs the requested actions '''

    # Build the command line argument parser
    parser = buildParser()
    # Read the arguments that were passed in
    args = parser.parse_args()
    bag = args.bag
    include = args.include
    exclude = args.exclude
    output = args.output

    do_work(bag, include, exclude, output)
