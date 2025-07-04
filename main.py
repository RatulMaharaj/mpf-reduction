from src.logger import logger
from src.columns import cols_to_keep
from src.config import (
    input_dir,
    local_temp_dir,
    out_dir,
    run_numbers,
    runs_of_interest,
    is_omp,
)
from src.process import process_rpt_file

# create an output dir
out_dir.mkdir(parents=True, exist_ok=True)
local_temp_dir.mkdir(parents=True, exist_ok=True)

# ensure the temp dir is empty
for file in local_temp_dir.glob("*"):
    file.unlink()

all_tasks = []

for run in runs_of_interest:
    for run_number in run_numbers:
        if is_omp:
            run_type = "NB" if run_number == "250" else "CLS"

            results_dir = (
                input_dir / f"#288.{run}" / run_type / run_number / f"RUN_{run_number}"  # noqa
            )
        else:
            results_dir = input_dir / f"#288.{run}" / f"RUN_{run_number}"
        rpts = list(results_dir.glob("*.rpt"))
        logger.info(f"Folder: {results_dir} - Found {len(rpts)} RPT files")

        for rpt_file in rpts:
            out_path = out_dir / f"#288.{run}" / f"RUN_{run_number}"
            out_file = out_path / f"{rpt_file.stem}.csv"

            if out_file.exists():
                logger.info(
                    f"skipping #288.{run} RUN_{run_number} {rpt_file.name}"  # noqa
                )
                continue

            all_tasks.append(
                (rpt_file, run, run_number, out_dir, local_temp_dir, cols_to_keep)  # noqa
            )

if __name__ == "__main__":
    logger.info("Starting MPF reduction process")
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(process_rpt_file, all_tasks)

    logger.info("MPF reduction process completed")
