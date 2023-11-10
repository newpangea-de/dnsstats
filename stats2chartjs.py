#!/usr/bin/env python3

########################################################################
# 
#   statprep.py
#
# Script that reads new CSV exports of statistics and merges them into a
# larger JSON statistics script
#
########################################################################

import argparse
import csv
import dataclasses
import json
import os
import sys
import tempfile

VERSION = "0.1.0"
PROGRAM = "stats2chartjs.py"
USAGE = """
TODO: Write down some usage description
"""

EPILOG = """
    part of the newpangea.de stack

    by Max Resing <max@newpangea.de>
"""

COLORS = [
    "#77aadd", "#00ddff", "#44bb99", "#bbcc33", "#aaaa00",
    "#eedd88", "#ee8866", "#ffaabb", "#dddddd",
]

COLORS_ALPHA = [
    "#77aadd99", "#00ddff99", "#44bb9999", "#bbcc3399", "#aaaa0099",
    "#eedd8899", "#ee886699", "#ffaabb99", "#dddddd99",
]


def load_raw(
        fpath: os.PathLike,
        stdin_fallback: bool = True,
        raise_err: bool = True,
) -> list[str]:
    """Loads a file to a string. Optionally falls back to STDIN, if
    fpath is empty.
    """
    raw_input = None

    if not fpath and stdin_fallback:
        raw_input = sys.stdin.readlines()
    elif os.path.isfile(fpath):
        with open(fpath, "r") as f:
            raw_input = f.readlines()
    
    if raw_input is None and raise_err:
        raise ValueError("Not a valid input file")

    return raw_input

@dataclasses.dataclass
class Dataset:
    label: str
    data: list
    borderColor: str = COLORS[-1]
    backgroundColor: str = COLORS_ALPHA[-1]
    fill: bool = False
    cubicInterpolationmode: str = "monotone"
    tension: float = 0.4


def append_rows_to_dict(data: dict, recs: list, cols: dict) -> dict:
    """
    """
    # Pivots are resolver and ts
    # datasets = [k for k in cols.keys() if k not in ["resolver", "ts"]]
    # timestamps = []

    datasets = dict([
        (dataset["label"], Dataset(**dataset))
        for dataset in data["datasets"]
    ])

    # The data set will look something like this:
    # {
    #     "datasets": [
    #         {
    #         "label": "dns1.cl.newpangea.de",
    #         "data": [
    #            { "ts": "2023-11-03", "qhosts": 123, "queries": 5000, "queries_distinct": 1000 },
    #            ...
    #         ],
    #         "borderColor": "#aabbcc",
    #         "backgroundColor": "#aabbcc99",
    #         "fill": false,
    #         "cubicInterpolationmode": "monotone",
    #         "tension: 0.4"
    #         },
    #     ]
    # }

    labels = data.get("labels", [])

    for rec in recs[1:]:
        if rec[cols["ts"]] not in labels:
            labels.append(rec[cols["ts"]])

        resolver = rec[cols["resolver"]]

        if resolver not in datasets.keys():
            datasets[resolver] = Dataset(label = resolver, data = [])

        datasets[resolver].data.append({
            col: float(rec[cols[col]]) if rec[cols[col]].isnumeric() else rec[cols[col]]
            for col in cols.keys() if col != "resolver"
        })
    
    for i, resolver in enumerate(datasets.keys()):
        datasets[resolver].label = resolver
        datasets[resolver].borderColor = COLORS[i]
        datasets[resolver].backgroundColor = COLORS_ALPHA[i]

    data["labels"] = labels
    data["datasets"] = [
        dataclasses.asdict(dataset)
        for dataset in datasets.values()
    ]

    return data


def main():
    parser = argparse.ArgumentParser(
        prog=PROGRAM,
        usage=USAGE,
        epilog=EPILOG,
    )

    parser.add_argument(
        "-o",
        "--output",
        type=str,
        help="The main file to merge the data into",
        required=True,
    )
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="Define an input path to a file to merge data into ; STDIN if none defined",
        required=False,
    )

    args = parser.parse_args(sys.argv[1:])

    raw_input = load_raw(args.input)
    raw_merge = load_raw(args.output, stdin_fallback=False, raise_err=False)

    # Load the file where data will be merged into
    statistics = json.loads("".join(raw_merge)) if raw_merge else {
        "labels": [],
        "datasets": [],
    }

    tmp_input  = tempfile.mkstemp()[1]
    with open(tmp_input, "w") as f:
        f.writelines(raw_input)

    with open(tmp_input, "r") as f:
        data_update = [l for l in csv.reader(f)]
        cols = dict([(c, i) for i, c in enumerate(data_update[0])])

    # Update the data
    statistics = append_rows_to_dict(statistics, data_update, cols)

    # Save the updated data to the output
    with open(args.output, "w") as f:
        json.dump(statistics, f, indent=2)

    # Cleanup
    os.remove(tmp_input)





if __name__ == "__main__":
    main()



