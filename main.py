import pandas as pd # noqa
import time
import shutil
from pathlib import Path
from src.io import read_rpt, write_chunked_csv
from src.logger import logger

input_dir = Path(R"\\OMRPRTP05.za.omlac.net\DEVELOPMENT\PE_Results\Segments\MFC RSA\2024-12\Mass Risk\Full") # noqa
local_temp_dir = Path(R"C:\Temp\MPF_Files")
out_dir = Path(R"out")
use_local_copy = True

run_numbers = ["179", "250"]
runs_of_interest = ["301", "302", "303", "304", "305", "306", "307", "308", "311", "312", "313", "314", "315"] # noqa
vars_to_keep = [
    "IFRS17_CONTRACT_ID",
    "LFRC_BEL",
    "LFRC_RA",
    "NO_POLS_MS(1:1)",
    "NO_POLS_MS(1:2)",
    "NO_POLS_MS(1:3)",
    "LFRC_BEL_COMPONENTS_I17(1)",
    "LFRC_BEL_COMPONENTS_I17(13)",
    "LFRC_BEL_COMPONENTS_I17(36)",
    "LFRC_BEL_COMPONENTS_I17(7)",
    "LFRC_BEL_COMPONENTS(15)",
    "LFRC_BEL_COMPONENTS(16)",
    "LFRC_BEL_COMPONENTS(17)",
    "LFRC_BEL_COMPONENTS(18)",
    "LFRC_BEL_COMPONENTS(19)",
    "LFRC_BEL_COMPONENTS(20)",
    "LFRC_BEL_COMPONENTS(21)",
    "LFRC_BEL_COMPONENTS(22)",
    "LFRC_BEL_COMPONENTS(23)",
    "LFRC_BEL_COMPONENTS(24)",
    "LFRC_BEL_COMPONENTS(25)",
    "LFRC_BEL_COMPONENTS(26)",
    "LFRC_BEL_COMPONENTS(27)",
    "LFRC_BEL_COMPONENTS(28)",
    "LFRC_BEL_COMPONENTS(29)",
    "LFRC_BEL_COMPONENTS(30)",
    "LFRC_BEL_COMPONENTS(51)",
    "REPORTING_DATA_DIMENSION(4)",
    "IFRS17_CONTRACT_ID",
    "LFRC_BEL",
    "LFRC_RA",
    "NO_POLS_MS(1:1)",
    "NO_POLS_MS(1:2)",
    "NO_POLS_MS(1:3)",
    "LFRC_BEL_COMPONENTS_I17(13)",
    "LFRC_BEL_COMPONENTS_I17(13)",
    "LFRC_BEL_COMPONENTS_I17(36)",
    "LFRC_BEL_COMPONENTS_I17(7)",
    "BEL_COMPONENTS(12)",
    "BEL_COMPONENTS(13)",
    "BEL_COMPONENTS(14)",
    "BEL_COMPONENTS(15)",
    "BEL_COMPONENTS(16)",
    "BEL_COMPONENTS(17)",
    "BEL_COMPONENTS(18)",
    "BEL_COMPONENTS(21)",
    "BEL_COMPONENTS(35)",
    "BEL_COMPONENTS(44)",
    "BEL_COMPONENTS(58)"
]

# create an output dir
out_dir.mkdir(parents=True, exist_ok=True)


def process_rpt_file(args):
    rpt_file, run, run_number, out_dir, local_temp_dir, vars_to_keep = args
    local_copy = local_temp_dir / rpt_file.name
    try:
        out_path = out_dir / f"#288.{run}" / f"RUN_{run_number}"
        out_file = out_path / f"{rpt_file.stem}.csv"

        if out_file.exists():
            logger.info(f"{rpt_file} already exists. skipping...")
            return

        out_path.mkdir(parents=True, exist_ok=True)

        if use_local_copy:
            logger.info(f"Copying {rpt_file} to {local_copy}")
            shutil.copy2(rpt_file, local_copy)

        start = time.perf_counter()
        if use_local_copy:
            logger.info(f"Reading {local_copy}")
        else:
            logger.info(f"Reading {rpt_file}")

        # Read and process in chunks
        chunk_size = 10000  # Adjust this based on your memory constraints
        output_columns = None  # Will be set after reading first chunk

        for i, chunk in enumerate(read_rpt(local_copy if use_local_copy else rpt_file, chunksize=chunk_size)): # noqa
            # Drop the header prefix column from the first chunk
            if i == 0:
                chunk = chunk.drop("!", axis=1)
                # Get intersection of available columns and vars_to_keep
                output_columns = [
                    col for col in vars_to_keep if col in chunk.columns
                ]

            # Write chunk to file
            mode = 'w' if i == 0 else 'a'
            header = i == 0
            write_chunked_csv(
                chunk,
                output_columns,
                out_file,
                mode=mode,
                header=header
            )

            logger.info(f"Processed chunk {i+1} of {rpt_file.name}")

        end = time.perf_counter()
        logger.info(f"Processed {rpt_file.name} in {end - start:.2f}s")

    except Exception as e:
        logger.error(f"Error with {rpt_file}: {e}")
    finally:
        if local_copy.exists():
            local_copy.unlink()


all_tasks = []
local_temp_dir.mkdir(parents=True, exist_ok=True)

for run in runs_of_interest:
    for run_number in run_numbers:
        results_dir = input_dir / f"#288.{run}" / f"RUN_{run_number}"
        rpts = list(results_dir.glob("*.rpt"))

        for rpt_file in rpts:
            all_tasks.append((
                rpt_file,
                run,
                run_number,
                out_dir,
                local_temp_dir,
                vars_to_keep
            ))


if __name__ == "__main__":
    logger.info("Starting MPF reduction process")
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(process_rpt_file, all_tasks)
    logger.info("MPF reduction process completed")
