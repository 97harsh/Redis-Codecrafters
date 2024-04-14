import time

def current_milli_time():
    return round(time.time() * 1000)

def convert_to_int(input):
    if isinstance(input, int):
        return input
    elif isinstance(input, str):
        return int(str)
    elif isinstance(input, bytes):
        return int(input)
    else:
        raise ValueError(f"Expected input to be int, bytes or integer, \
                            but found {input} of type {type(input)}")
    