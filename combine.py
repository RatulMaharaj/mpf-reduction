import pandas as pd
from pathlib import Path
from src.logger import logger
from src.io import write_chunked_csv


def combine_csv_files(
    input_dir: Path,
    output_file: Path,
    summary_file: Path,
    chunk_size: int = 10_000,
):
    """
    Combine all CSV files in the input directory into a single output file
    and generate summary statistics. Files are processed in chunks to manage
    memory usage.

    Args:
        input_dir: Directory containing CSV files to combine
        output_file: Path to write the combined file
        summary_file: Path to write the summary statistics
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

    # Process each file
    is_first_chunk = True
    total_rows = 0
    summary_data = []

    for csv_file in csv_files:
        logger.info(f"Processing {csv_file.name}")

        # Initialize summary statistics for this file
        file_row_count = 0
        file_lfrc_bel_sum = 0
        file_lfrc_ra_sum = 0

        # Process file in chunks
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size):
            # Update summary statistics
            file_row_count += len(chunk)
            file_lfrc_bel_sum += chunk["LFRC_BEL"].sum()
            file_lfrc_ra_sum += chunk["LFRC_RA"].sum()

            # Write chunk to output file
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

        # Check if file might be incomplete
        is_error_file = file_row_count % 10_000 == 0

        # Add file summary to data
        summary_data.append(
            {
                "file_name": csv_file.name,
                "row_count": file_row_count,
                "lfrc_bel_sum": file_lfrc_bel_sum,
                "lfrc_ra_sum": file_lfrc_ra_sum,
                "is_error_file": is_error_file,
            }
        )

        if is_error_file:
            msg = (
                f"File {csv_file.name} has {file_row_count} rows "
                "(divisible by 10k) - possible incomplete file"
            )
            logger.warning(msg)

    # Write summary statistics
    summary_df = pd.DataFrame(summary_data)
    summary_df.to_csv(summary_file, index=False)
    logger.info(f"Summary statistics written to {summary_file}")

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
            summary_file = out_dir / f"summary_#288.{run}_RUN_{run_number}.csv"

            if output_file.exists():
                # ask user to delete the file if they want to rerun
                logger.warning(
                    f"File {output_file} already combined. Skipping.."  # noqa
                )
                continue

            logger.info(f"Processing files for #288.{run}_RUN_{run_number}")
            combine_csv_files(run_dir, output_file, summary_file)
