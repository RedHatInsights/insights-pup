#!/usr/bin/env python3

"""
This is an example of how to use insights-core to validate a tarball and
extract a few canonical facts from it.
"""

from insights import run, extract
from insights.specs import Specs

canonical_facts = [
    Specs.machine_id,
    Specs.hostname,
    Specs.redhat_release,
    Specs.uname,
]


def get_archive_name():
    import sys
    import os

    if len(sys.argv) < 2:
        print("Need archive name")
        sys.exit(1)

    archive_name = sys.argv[1]

    if not os.path.exists(archive_name):
        print(f"Invalid archive path: {archive_name}")
        sys.exit(1)

    return archive_name


if __name__ == "__main__":
    with extract(get_archive_name()) as ex:
        broker = run(root=ex.tmp_dir)
        for fact in canonical_facts:
            print("\n".join(broker[fact].content))
