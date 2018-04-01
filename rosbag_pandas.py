#!/usr/bin/env python

import re
import subprocess
import warnings

import numpy as np
import pandas as pd
import rosbag
import yaml
from box import Box
from six import string_types


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


def flatten_dict(d):
    def expand(key, value):
        if isinstance(value, dict):
            return [(key + '.' + k, v) for k, v in flatten_dict(value).items()]
        elif isinstance(value, (tuple, list)):
            parsed_collection = [("{0}[{1}]".format(key, idx), v) for idx, v in enumerate(value)]
            return [(k + '.' + fk, fv) for k, v in parsed_collection for fk, fv in flatten_dict(v).items()]
        else:
            return [(key, value)]

    items = [item for k, v in d.items() for item in expand(k, v)]
    return dict(items)


def bag_to_dataframe(bag_name, fields=None, include=None, exclude=None, seconds=False):
    '''
    Read in a rosbag file and create a pandas data frame that
    is indexed by the time the message was recorded in the bag.

    :bag_name: String name for the bag file
    :fields: None, String  Fields to include in the dataframe
               if None all topics added, if string it is used as regular
                   expression.
    :include: None, String, or List  Topics to include in the dataframe
               if None all topics added, if string it is used as regular
                   expression, if list that list is used.
    :exclude: None, String, or List  Topics to be removed from those added
            using the include option using set difference.  If None no topics
            removed. If String it is treated as a regular expression. A list
            removes those in the list.

    :seconds: time index is in seconds

    :returns: a list of pandas dataframe objects
    '''
    # get list of topics to parse
    yaml_info = get_bag_info(bag_name)
    bag_topics = get_topics(yaml_info)
    bag_topics = prune_topics(bag_topics, include, exclude)
    bag = rosbag.Bag(bag_name)

    # create datastore and index
    datastore = {}
    index = {}

    # all of the data is loaded
    for topic, msg, mt in bag.read_messages(topics=bag_topics):
        msg_as_dict = to_dict(msg)
        parsed_msg = Box(msg_as_dict)
        flatten_msg = flatten_dict(msg_as_dict)

        time = -1
        try:
            if seconds:
                time = parsed_msg.header.stamp.secs
            else:
                time = parsed_msg.header.stamp.nsecs
        except:
            if seconds:
                time = mt.to_sec()
            else:
                time = mt.to_nsec()

        for k, v in flatten_msg.items():
            if not isinstance(v, (int, float)):
                continue

            final_key = get_key_name(topic, k)
            if not valid_field(final_key, fields):
                continue

            if topic not in datastore:
                datastore[topic] = {}
            if final_key not in datastore[topic]:
                datastore[topic][final_key] = []

            datastore[topic][final_key].append(v)

        if topic not in index:
            index[topic] = []

        index[topic].append(time)

    bag.close()

    dataframes = []
    for topic, fields in datastore.items():
        data_result = {}
        index_result = index[topic]

        # convert the index
        if not seconds:
            index_result = pd.to_datetime(index_result, unit='ns')

        for field_name, field_value in fields.items():
            data_result[field_name] = np.array(field_value)

        dataframes.append(pd.DataFrame(data=data_result, index=index_result))

    # now we have read all of the messages its time to assemble the dataframe
    return dataframes


def valid_field(input_field, valid_fields):
    if valid_fields is None:
        return True

    for pattern in valid_fields:
        check = re.compile(pattern)
        if re.match(check, input_field) is not None:
            return True

    return False


def prune_topics(bag_topics, include, exclude):
    '''prune the topics.  If include is None add all to the set of topics to
       use if include is a string regex match that string,
       if it is a list use the list

        If exclude is None do nothing, if string remove the topics with regex,
        if it is a list remove those topics'''

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
    '''Get uamle dict of the bag information
    by calling the subprocess -- used to create correct sized
    arrays'''
    # Get the info on the bag
    bag_info = yaml.load(subprocess.Popen(
        ['rosbag', 'info', '--yaml', bag_file],
        stdout=subprocess.PIPE).communicate()[0])
    return bag_info


def get_topics(yaml_info):
    ''' Returns the names of all of the topics in the bag, and prints them
        to stdout if requested
    '''
    # Pull out the topic info
    names = []
    # Store all of the topics in a dictionary
    topics = yaml_info['topics']
    for topic in topics:
        names.append(topic['topic'])

    return names


def get_key_name(topic, path):
    '''fix up topic to key names to make them a little prettier'''
    return "{0}/{1}".format(topic, path.replace(".", "/"))


if __name__ == '__main__':
    print('hello')
