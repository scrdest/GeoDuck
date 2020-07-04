import os

import constants as const


def to_jsonl(iterable, base_filename=None):
    import json
    _base_filename = base_filename or os.path.join(const.OUTPUT_DIR, 'result')
    json_stream = map(json.dumps, iterable)

    files = {}
    try:
        for (idx, item) in enumerate(iterable):
            print(idx)
            filename = '{base}_{num}.json'.format(
                base=_base_filename,
                num=(idx // 1000)
            )
            fd = files.setdefault(filename, open(filename, 'w'))
            fd.write(json.dumps(item))
            fd.write('\n')
    finally:
        for fd in files.values():
            fd.close()
    return True