from core import main
import constants as const

def run():
    from interface import cli
    raw_app_args = cli.read_args()
    qry_term = '+AND+'.join(
        raw_app_args.get(const.ARG_QUERY) 
        or ["human[orgn]"]
    )
    main(term=qry_term, db='gds') 
