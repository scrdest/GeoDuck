import sys
from constants import BASE_DIR
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

from app.core.run import run
run() 
