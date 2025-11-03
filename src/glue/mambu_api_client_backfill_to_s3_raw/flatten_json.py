import json
import sys

import six


def _construct_key(previous_key, separator, new_key, replace_separators=None):
    """
    Returns the new_key if no previous key exists, otherwise concatenates
    previous key, separator, and new_key
    :param previous_key:
    :param separator:
    :param new_key:
    :param str replace_separators: Replace separators within keys
    :return: a string if previous_key exists and simply passes through the
    new_key otherwise
    """
    if replace_separators is not None:
        new_key = str(new_key).replace(separator, replace_separators)
    if previous_key:
        return "{}{}{}".format(previous_key, separator, new_key)
    else:
        return new_key


def flatten(
    nested_dict, separator="_", root_keys_to_ignore=None, replace_separators=None
):
    """
    Flattens a dictionary with nested structure to a dictionary with no
    hierarchy
    Consider ignoring keys that you are not interested in to prevent
    unnecessary processing
    This is specially true for very deep objects

    :param nested_dict: dictionary we want to flatten
    :param separator: string to separate dictionary keys by
    :param root_keys_to_ignore: set of root keys to ignore from flattening
    :param str replace_separators: Replace separators within keys
    :return: flattened dictionary
    """
    assert isinstance(nested_dict, dict), "flatten requires a dictionary input"
    assert isinstance(separator, six.string_types), "separator must be string"

    if root_keys_to_ignore is None:
        root_keys_to_ignore = set()

    if len(nested_dict) == 0:
        return {}

    # This global dictionary stores the flattened keys and values and is
    # ultimately returned
    flattened_dict = dict()

    def _flatten(object_, key):
        """
        For dict, list and set objects_ calls itself on the elements and for
        other types assigns the object_ to
        the corresponding key in the global flattened_dict
        :param object_: object to flatten
        :param key: carries the concatenated key for the object_
        :return: None
        """
        # Empty object can't be iterated, take as is
        if not object_:
            flattened_dict[key] = object_
        # These object types support iteration
        elif isinstance(object_, dict):
            for object_key in object_:
                if not (not key and object_key in root_keys_to_ignore):
                    _flatten(
                        object_[object_key],
                        _construct_key(
                            key,
                            separator,
                            object_key,
                            replace_separators=replace_separators,
                        ),
                    )
        elif isinstance(object_, (list, set, tuple)):
            for index, item in enumerate(object_):
                _flatten(
                    item,
                    _construct_key(
                        key, separator, index, replace_separators=replace_separators
                    ),
                )
        # Anything left take as is
        else:
            flattened_dict[key] = object_

    _flatten(nested_dict, None)
    return flattened_dict


flatten_json = flatten


def cli(input_stream=sys.stdin, output_stream=sys.stdout):
    raw = input_stream.read()
    input_json = json.loads(raw)
    output = json.dumps(flatten(input_json))
    output_stream.write("{}\n".format(output))
    output_stream.flush()


if __name__ == "__main__":
    cli()
