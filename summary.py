from src.config import out_dir
import pandas as pd


def main():
    df = pd.DataFrame()
    for file in out_dir.glob("summary_*.csv"):
        df_temp = pd.read_csv(file)
        # add a column for the file name
        df_temp["run_info"] = file.stem
        df = pd.concat([df, df_temp])

    df.to_csv(out_dir / "all_runs.csv", index=False)


if __name__ == "__main__":
    main()
