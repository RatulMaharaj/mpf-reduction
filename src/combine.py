import pandas as pd
from pathlib import Path
from src.logger import logger
from src.io import write_chunked_csv


def combine_csv_files(
    input_dir: Path,
    output_file: Path,
    chunk_size: int = 10_000,
):
    """
    Combine all CSV files in the input directory into a single output file.
    Files are processed in chunks to manage memory usage.

    Args:
        input_dir: Directory containing CSV files to combine
        output_file: Path to write the combined file
        chunk_size: Number of rows to process at once
    """
    # Get all CSV files in the directory
    csv_files = list(input_dir.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in {input_dir}")
        return

    logger.info(f"Found {len(csv_files)} CSV files to combine")

    # Read first file to get columns
    first_file = pd.read_csv(csv_files[0], nrows=0)
    output_columns = first_file.columns.tolist()
    logger.info(f"Using columns: {output_columns}")

    # Process each file in chunks
    is_first_chunk = True
    total_rows = 0

    for csv_file in csv_files:
        logger.info(f"Processing {csv_file.name}")

        # Read and write file in chunks
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            write_chunked_csv(
                chunk,
                output_columns,
                output_file,
                mode="w" if is_first_chunk else "a",
                header=is_first_chunk,
            )

            total_rows += len(chunk)
            logger.info(f"Processed {len(chunk)} rows from {csv_file.name}")
            is_first_chunk = False

    logger.info(f"Combined {len(csv_files)} files into {output_file}")
    logger.info(f"Total rows in combined file: {total_rows}")


if __name__ == "__main__":
    from src.config import out_dir, run_numbers, runs_of_interest

    # Combine files for each run number
    for run in runs_of_interest:
        for run_number in run_numbers:
            run_dir = out_dir / f"#288.{run}" / f"RUN_{run_number}"
            if not run_dir.exists():
                logger.warning(f"Directory {run_dir} does not exist, skipping...")  # noqa
                continue

            output_file = out_dir / f"combined_#288.{run}_RUN_{run_number}.csv"
            logger.info(f"Combining files for #288.{run} RUN_{run_number}")
            combine_csv_files(run_dir, output_file)
