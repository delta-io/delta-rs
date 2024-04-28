import polars
from deltalake import DeltaTable

dt = DeltaTable("../rust/tests/data/cdf-table")
table = dt.load_cdf(starting_version=0, ending_version=4).read_all()
pt = polars.from_arrow(table)
pt.group_by("_commit_version").len().sort("len", descending=True)
