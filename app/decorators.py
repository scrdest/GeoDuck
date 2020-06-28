import os
import sys
from functools import wraps

try: import constants as const
except ImportError: import app.constants as const

def with_print(pretty=False, disabled=False, to_stream=None):

    def _with_print_deco(func):
    
        @wraps(func)
        def _printwrapper(*args, **kwargs):
            printer = lambda x, s, **pkwargs: print(x, file=(s or sys.stdout), **pkwargs)
            print_kwargs = dict()
            
            if pretty:
                from pprint import pprint
                printer = lambda x, s, **pkwargs: pprint(x, stream=(s or sys.stdout), **pkwargs)
                
            result = func(*args, **kwargs)
            
            if isinstance(to_stream, str):
                with open(to_stream, ('w')) as _fileout:
                    printer(result, _fileout)
            else:
                printer(result, to_stream)
                
            return result
            
        return func if disabled else _printwrapper
        
    return _with_print_deco


def result_to_json(filename=None, disabled=False):
    import json

    def _jsondump_deco(func):
    
        @wraps(func)
        def _resultwrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            
            import json
            _filename = filename or os.path.join(const.OUTPUT_DIR, 'result.json')
            with open(_filename, 'w') as jsonfile:
                writer = json.dump(result, jsonfile)
                
            return result
            
        return func if disabled else _resultwrapper
        
    return _jsondump_deco
