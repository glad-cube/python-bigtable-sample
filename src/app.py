# coding: utf-8
from __future__ import print_function
from google.cloud import bigtable
from google.cloud import happybase
import random


def main():
    # connect to bigtable
    project_id = "gcp_project_id"
    instance_id = "bigtable_instance_id"
    client = bigtable.Client(project=project_id, admin=True)
    instance = client.instance(instance_id)
    connection = happybase.Connection(instance=instance)

    # create table
    table_name = "apps"
    column_family_name = "data"
    try:
        connection.create_table(table_name, {
            column_family_name: dict()
        })
    except Exception as e:
        print(e.message)

    # create rows
    table = connection.table(table_name)
    names = ["John", "Jane", "Richard", "Alan"]
    for name in names:
        table.put(name, {"data:name": name, "data:age": random.randint(20, 40)})

    # get data
    print(table.row(names[0]))

    for row in table.rows(names[2:]):
        print(row)

    for row in table.scan(row_prefix="J"):
        print(row)


if __name__ == "__main__":
    main()
