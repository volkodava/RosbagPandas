#!/usr/bin/env python
# encoding: utf-8

import matplotlib.pyplot as plt

import rosbag_pandas


def parse_series_args(topics, fields):
    '''Return which topics and which field keys need to be examined
    for plotting'''
    return [topic for field in fields for topic in topics if field.startswith(topic)]


def graph(dfs, fields):
    fig, axes = plt.subplots(len(fields), sharex=True)
    idx = 0
    for df in dfs:
        for field in df.columns:
            s = df[field].dropna()
            axes[idx].plot(s.index - s.index.min(), s.values)
            axes[idx].set_title(field)
            idx = idx + 1
    plt.show()


if __name__ == '__main__':
    ''' Main entry point for the function. Reads the command line arguements
    and performs the requested actions '''
    bag = "/work/1118382_2018-03-28-16-30-52.bag"
    input_fields = [
        "/twist_cmd/twist/linear/x$",
        "/current_velocity/twist/linear/x$",
        "/vehicle/throttle_cmd/pedal_cmd$",
        "/vehicle/brake_cmd/pedal_cmd$",
        # "/base_waypoints/waypoints\[.*\]/twist/twist/linear/x$"
    ]
    # fields = ["/base_waypoints/twist/twist/linear/x"]
    yaml_info = rosbag_pandas.get_bag_info(bag)
    topics = rosbag_pandas.get_topics(yaml_info)
    matched_topics = parse_series_args(topics, input_fields)
    dfs = rosbag_pandas.bag_to_dataframe(bag, fields=input_fields, include=matched_topics, seconds=True)
    graph(dfs, input_fields)
