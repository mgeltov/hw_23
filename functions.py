import os

def filter_query(value, data):
    return filter(lambda l: value in l, data)


def map_query(value, data):
    return map(lambda l: l.split(' ')[int(value)], data)


def unique_query(data, *args, **kwargs):
    return set(data)


def sort_query(value, data):
    if value == 'desc':
        reverse = True
    else:
        reverse = False
    return sorted(data, reverse=reverse)


def limit_query(value, data):
    return list(data)[:int(value)]


def read_file(filename):
    with open(filename) as file:
        for line in file:
            yield line


dict_comm_to_function = {
    'filter': filter_query,
    'map': map_query,
    'unique': unique_query,
    'sort': sort_query,
    'limit': limit_query
}


def create_query(filename, cmd, value, data):
    if data is None:
        data_to_proceed = read_file(filename)
    else:
        data_to_proceed = data

    function = dict_comm_to_function[cmd]
    res = function(value=value, data=data_to_proceed)

    return list(res)