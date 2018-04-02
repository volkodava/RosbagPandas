#!/usr/bin/env python
# encoding: utf-8
import json
import re
import subprocess
import warnings

import rosbag
import yaml
from six import string_types


def bag_to_json(bag_name, include=None, exclude=None):
    """
    Read in a rosbag file and convert messages to json.

    :bag_name: String  name for the bag file
    :include: None, String, or List  Topics to include in the dataframe
               if None all topics added, if string it is used as regular
                   expression, if list that list is used.
    :exclude: None, String, or List  Topics to be removed from those added
            using the include option using set difference.  If None no topics
            removed. If String it is treated as a regular expression. A list
            removes those in the list.

    :returns: a serialized json messages
    """

    # get list of topics to parse
    yaml_info = get_bag_info(bag_name)
    bag_topics = get_topics(yaml_info)
    bag_topics = prune_topics(bag_topics, include, exclude)
    bag = None
    datastore = {}
    try:
        bag = rosbag.Bag(bag_name)
        for topic, msg, mt in bag.read_messages(topics=bag_topics):
            if topic not in datastore:
                datastore[topic] = []

            datastore[topic].append({"data": to_dict(msg), "secs": mt.to_sec(), "nsecs": mt.to_nsec()})
    finally:
        if bag:
            bag.close()

    return json.dumps(datastore)


def prune_topics(bag_topics, include, exclude):
    """
    prune the topics.  If include is None add all to the set of topics to
    use if include is a string regex match that string,
    if it is a list use the list

    If exclude is None do nothing, if string remove the topics with regex,
    if it is a list remove those topics
    """

    topics_to_use = set()
    # add all of the topics
    if include is None:
        for t in bag_topics:
            topics_to_use.add(t)
    elif isinstance(include, string_types):
        check = re.compile(include)
        for t in bag_topics:
            if re.match(check, t) is not None:
                topics_to_use.add(t)
    else:
        try:
            # add all of the includes if it is in the topic
            for topic in include:
                if topic in bag_topics:
                    topics_to_use.add(topic)
        except:
            warnings.warn('Error in topic selection Using All!')
            topics_to_use = set()
            for t in bag_topics:
                topics_to_use.add(t)

    to_remove = set()
    # now exclude the exclusions
    if exclude is None:
        pass
    elif isinstance(exclude, string_types):
        check = re.compile(exclude)
        for t in list(topics_to_use):
            if re.match(check, t) is not None:
                to_remove.add(t)
    else:
        for remove in exclude:
            if remove in exclude:
                to_remove.add(remove)

    # final set stuff to get topics to use
    topics_to_use = topics_to_use - to_remove
    # return a list for the results
    return list(topics_to_use)


def get_bag_info(bag_file):
    """
    Get uamle dict of the bag information
    by calling the subprocess -- used to create correct sized
    arrays
    """

    # Get the info on the bag
    bag_info = yaml.load(subprocess.Popen(
        ['rosbag', 'info', '--yaml', bag_file],
        stdout=subprocess.PIPE).communicate()[0])
    return bag_info


def get_topics(yaml_info):
    """
    Returns the names of all of the topics in the bag, and prints them
    to stdout if requested
    """

    # Pull out the topic info
    names = []
    # Store all of the topics in a dictionary
    topics = yaml_info['topics']
    for topic in topics:
        names.append(topic['topic'])

    return names


def to_dict(obj):
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = to_dict(v)
        return data
    elif hasattr(obj, "_ast"):
        return to_dict(obj._ast())
    elif hasattr(obj, "__iter__"):
        return [to_dict(v) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, to_dict(value))
                     for key, value in obj.__dict__.iteritems()
                     if not callable(value) and not key.startswith('_')])
        return data
    else:
        data = {}
        for name in dir(obj):
            if not name.startswith('_'):
                value = getattr(obj, name)
                if isinstance(value, (int, float, bool, str)):
                    data[name] = value
                elif not callable(value):
                    data[name] = to_dict(value)
        return data


if __name__ == '__main__':
    print('hello')
