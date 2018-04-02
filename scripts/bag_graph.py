#!/usr/bin/env python
# encoding: utf-8

import matplotlib.pyplot as plt

import rosbag_pandas


def parse_series_args(topics, fields):
    '''Return which topics and which field keys need to be examined
    for plotting'''
    return [topic for field in fields for topic in topics if field.startswith(topic)]


def graph(dfs, sharex=True, start_time=None, stop_time=-1):
    fig, axes = plt.subplots(len(dfs), sharex=sharex, squeeze=False)
    idx = 0
    for result in dfs:
        df = result.getDataFrame(start_time, stop_time)
        for column in df.columns:
            s = df[column].dropna()
            axes[idx, 0].plot(s.index, s.values)
            axes[idx, 0].set_title(result.title)
            idx = idx + 1
    plt.tight_layout()
    plt.show()


if __name__ == '__main__':
    ''' Main entry point for the function. Reads the command line arguements
    and performs the requested actions '''
    bag = "/work/1118382_2018-03-28-16-30-52.bag"
    mapping_rules = {
        # "/twist_cmd/twist/linear/x": "twist.linear.x",
        # "/current_velocity/twist/linear/x": "twist.linear.x",
        # "/vehicle/throttle_cmd/pedal_cmd": "pedal_cmd",
        # "/vehicle/brake_cmd/pedal_cmd": "pedal_cmd",
        "/vehicle/dbw_enabled": "data",
        # "/traffic_waypoint/data": "data",
    }
    # mapping_rules = {
    #     # "/base_waypoints/waypoints/twist/twist/linear/x": "waypoints[*].twist.twist.linear.x",
    #     "/final_waypoints/waypoints/twist/twist/linear/x": "waypoints[*].twist.twist.linear.x",
    # }
    yaml_info = rosbag_pandas.get_bag_info(bag)
    topics = rosbag_pandas.get_topics(yaml_info)
    matched_topics = parse_series_args(topics, mapping_rules.keys())
    dfs = rosbag_pandas.bag_to_dataframe(bag, mapping_rules=mapping_rules, include=matched_topics, seconds=True)
    # graph(dfs, sharex=True, start_time=102, stop_time=103)
    # graph(dfs, sharex=True)
    # for time, dbw_enabled in zip(dfs[0].index, dfs[0].data):
    #     if dbw_enabled:
    #         print("{: >10} sec. {: >10}".format(int(time), "manual"))
    #     else:
    #         print("{: >10} sec. {: >10}".format(int(time), "auto"))
