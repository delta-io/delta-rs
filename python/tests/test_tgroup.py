import os
from deltalake import DeltaTable


def test_tgroup():
    table_path = "/home/naveen/Projects/delta-rs-mt/crates/test/tests/data/t_group_table_2"
    logdir = f"{table_path}/_delta_log"
    tgroup_path = "/home/naveen/Projects/delta-rs-mt/crates/test/tests/data/t_group_1"
    tgroup_logdir = f"{tgroup_path}/_delta_log"
    print(f"Initializing delta table at {table_path}...")
    dt = DeltaTable(table_path)

    log_files_pre = get_log_files(logdir)
    list_log_files(logdir)
    tgroup_logs_pre = get_log_files(tgroup_logdir)
    list_log_files(tgroup_logdir)

    print(f"\nAdding table to t-group at {tgroup_path}...")
    dt.add_to_tgroup(tgroup_path)

    # log_files_post = get_log_files(logdir)
    # list_log_files(logdir)
    # tgroup_logs_post = get_log_files(tgroup_logdir)
    # list_log_files(tgroup_logdir)
    # delete_log_files(logdir, set(log_files_post) - set(log_files_pre))
    # delete_log_files(tgroup_logdir, set(tgroup_logs_post) - set(tgroup_logs_pre))

def test_cleanup():
    table_path = "/home/naveen/Projects/delta-rs-mt/crates/test/tests/data/t_group_table_2"
    logdir = f"{table_path}/_delta_log"
    tgroup_path = "/home/naveen/Projects/delta-rs-mt/crates/test/tests/data/t_group_1"
    tgroup_logdir = f"{tgroup_path}/_delta_log"

    log_files = get_log_files(logdir)
    tgroup_logs = get_log_files(tgroup_logdir)

    logs_to_delete = set(log_files) - set([
        "00000000000000000000.json", 
        "00000000000000000001.json", 
        "00000000000000000002.json", 
        "00000000000000000003.json", 
        "00000000000000000004.json"
    ])

    tgroup_logs_to_delete = set(tgroup_logs) - set(["00000000000000000000.json"])

    delete_log_files(logdir, logs_to_delete)
    delete_log_files(tgroup_logdir, tgroup_logs_to_delete)


def get_log_files(logdir: str) -> list[str]:
    return [f for f in os.listdir(logdir) if os.path.isfile(os.path.join(logdir, f))]

def list_log_files(logdir: str):
    print(f"\nListing files in {logdir}:")
    for file in get_log_files(logdir):
        print(f"\t{file}")

def delete_log_files(logdir: str, log_files: list[str]):
    for file in log_files:
        os.remove(os.path.join(logdir, file))