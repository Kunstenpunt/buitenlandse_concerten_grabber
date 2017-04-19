from pandas import read_excel
from glob import glob
from pandas import DataFrame, concat

data_file_name = "output/buitenlandse_concerten.xlsx"

tabel_oud_df = read_excel(data_file_name) if data_file_name in glob("output/*.xlsx") else DataFrame()

bit = read_excel("providers/bandsintown.xlsx")
sk = read_excel("providers/songkick.xlsx")
# vi = read_excel("providers/vibe.xlsx")
# ra = read_excel("providers/residentadvisor.xlsx")

tabel_nieuw_df = concat([bit, sk])

diff = concat([tabel_oud_df, tabel_nieuw_df])
diff = diff.reset_index(drop=True)
diff_gpby = diff.groupby(list(diff.columns))
idx = [x[0] for x in diff_gpby.groups.values() if len(x) == 1]
diff.reindex(idx).to_excel("output/diff.xlsx", index=False)

tabel_nieuw_df.drop_duplicates(subset=["artiest", "datum", "land", "stad"]).to_excel("output/buitenlandse_concerten.xlsx", index=False)
