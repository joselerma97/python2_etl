import argparse

from etl.Aggregate import Aggregate
from etl.Enrich import Enrich
from etl.Preproc import Preproc
from etl.Raw import Raw

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Process")
    parser.add_argument("--name", type=str, help="The name of the file in which store the macrotable")
    parser.add_argument("--start", type=str, help="The starting date to consider to build the macrotable")
    parser.add_argument("--end", type=str, help="The ending date to consider to build the macrotable")
    parser.add_argument("--countries", type=str, help="The list of countries to include in the macrotable")

    args = parser.parse_args()

    print(args.name)
    print(args.start)
    print(args.end)
    if args.countries:
        print(args.countries.split(","))
    else:
        print([])

    aggregate = Aggregate(args.name)
    aggregate.handle_action()
    enrich = Enrich(args.countries.split(",") if args.countries else [], aggregate)
    pre_proc = Preproc(args.start, args.end, enrich)
    raw = Raw(pre_proc)
    raw.handle_action()
