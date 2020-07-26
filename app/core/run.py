from core import main
import constants as const

CLI_ARG_FULLQUERY = 'query'


def parse_cli(cli_args: dict) -> dict:
    results = dict()

    free_text_query = '+AND+'.join((
        cli_args.get(const.ARG_QUERY)
        or []
    ))

    organism_query = '{species}[Organism]'.format(
        species=(
            cli_args.get(const.ARG_ORGANISM)
            or 'human'
        )
    )

    qry_term = '+AND+'.join((free_text_query, organism_query, 'gsm[EntryType]', 'csv[Supplementary Files]'))
    results[CLI_ARG_FULLQUERY] = qry_term
    return results


def run():
    from interface import cli
    raw_app_args = cli.read_args()
    parsed_args = parse_cli(raw_app_args)
    qry_term = parsed_args[CLI_ARG_FULLQUERY]

    status = main(term=qry_term, db='gds')
    return status
