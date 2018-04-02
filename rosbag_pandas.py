#!/usr/bin/env python

import re
import subprocess
import warnings

import jmespath
import numpy as np
import pandas as pd
import rosbag
import yaml
from six import string_types


class DataframeResult:
    def __init__(self, title, data, index):
        self.title = title
        self.data = np.array(data)
        self.index = np.array(index)
        self.index = self.index - self.index.min()

    def getDataFrame(self, start_time=None, stop_time=-1):
        data = None
        index = None
        if start_time is None:
            data = np.array(self.data)
            index = np.array(self.index)
        else:
            time_idx_start = self.find_nearest_index(self.index, start_time)
            time_idx_stop = self.find_nearest_index(self.index, stop_time)
            data = np.array(self.data[time_idx_start:time_idx_stop])
            index = np.arange(data.size)

        return pd.DataFrame(data=data, index=index)

    def find_nearest_index(self, arr, value):
        return (np.abs(arr - value)).argmin()


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


def bag_to_dataframe(bag_name, mapping_rules, include=None, exclude=None, seconds=False):
    '''
    Read in a rosbag file and create a pandas data frame that
    is indexed by the time the message was recorded in the bag.

    :bag_name: String name for the bag file
    :mapping_rules: Dict  Defines mapping rules to extract information
               from the message, where key - field path, value - JMESPath expression.
    :include: None, String, or List  Topics to include in the dataframe
               if None all topics added, if string it is used as regular
                   expression, if list that list is used.
    :exclude: None, String, or List  Topics to be removed from those added
            using the include option using set difference.  If None no topics
            removed. If String it is treated as a regular expression. A list
            removes those in the list.

    :seconds: time index is in seconds

    :returns: a list of dataframe results
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
        data_result = extract_data(msg_as_dict, mapping_rules, topic, mt)

        for field_path, field_value in data_result["data"].items():
            if field_path not in datastore:
                datastore[field_path] = []
            if field_path not in index:
                index[field_path] = []

            datastore[field_path].append(field_value)
            if seconds:
                index[field_path].append(data_result["secs"])
            else:
                index[field_path].append(data_result["nsecs"])

    bag.close()

    dataframes = []
    for field_path, values in datastore.items():
        data_result = values
        index_result = index[field_path]

        # convert the index
        if not seconds:
            index_result = pd.to_datetime(index_result, unit='ns')

        dataframes.append(DataframeResult(field_path, data_result, index_result))

    # now we have read all of the messages its time to assemble the dataframe
    return sorted(dataframes, key=lambda x: x.title)


def extract_data(msg_as_dict, mapping_rules, topic, mt):
    if not mapping_rules:
        return None

    data = {}
    for field_path, jmes_path in mapping_rules.items():
        if field_path.startswith(topic):
            search_results = jmespath.search(jmes_path, msg_as_dict)
            if isinstance(search_results, (int, float)) or search_results:
                data[field_path] = search_results

    return {"data": data, "secs": mt.to_sec(), "nsecs": mt.to_nsec()}


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



if __name__ == '__main__':
    print('hello')
