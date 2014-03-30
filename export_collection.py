"""Export collections"""

import config


def main():
    # TODO not finished, only print out the commands
    db_config = config.DB_CONFIG
    server = db_config["export_collection_server"]
    collections = db_config["target_collections"]
    cmds = []
    for coll in collections:
        cmd = "mongoexport -h {0} -p {1} -c {2} -o {2}.json -d {3}".format(
            server["host"], server["port"], coll, db_config["target_database"])
        cmds.append(cmd)
    print ' &&\n'.join(cmds)

if __name__ == '__main__':
    main()
