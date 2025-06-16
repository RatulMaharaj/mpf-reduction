import pandas as pd
from src.config import out_dir

run_info_df = pd.read_csv(out_dir / "all_runs.csv")

# remove rows where error_code is not 0
run_info_df = run_info_df[run_info_df["is_error_file"]]
# iterate over the rows and print the file name
for index, row in run_info_df.iterrows():
    # extract the file name from the run_info
    run_number = row["run_info"].split("_")[-1]
    run_name = row["run_info"].split("_")[1]

    folder_path = out_dir / run_name / f"RUN_{run_number}"

    file_path = folder_path / row["file_name"]
    # remove the file
    if file_path.exists():
        print(f"Removing file {file_path}")
        file_path.unlink()
    else:
        print(f"File {file_path} does not exist")
