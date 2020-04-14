import datetime
from pathlib import Path


def log_message(*args, p_fn=print):
    text = " ".join([str(x) for x in args])
    p_fn(f'{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}: {text}')


def verify_pathtype(urltable_path):
    if isinstance(urltable_path, str):
        return Path(urltable_path)
    elif isinstance(urltable_path, Path):
        return urltable_path
    else:
        raise TypeError("input must be type pathlib.Path() or str ")
