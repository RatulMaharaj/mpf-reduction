import time
import shutil
from src.io import write_chunked_csv, stream_rpt_file
from src.config import use_local_copy
from src.logger import logger


def process_rpt_file(args):
    rpt_file, run, run_number, out_dir, local_temp_dir, cols_to_keep = args
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

        # Process file in chunks using streaming
        chunk_size = 10_000
        output_columns = None
        is_first_chunk = True  # Renamed for clarity

        for chunk in stream_rpt_file(
            local_copy if use_local_copy else rpt_file, chunk_size=chunk_size
        ):
            msg = f"Processing chunk of {rpt_file.name} with {len(chunk)} rows"
            logger.info(msg)

            # chunk level manipulations
            # split IFRS17_CONTRACT_ID into 3 columns
            # 69410S705341618_1_477 -> 69410S705341618, 1, 477
            split_contract = chunk["IFRS17_CONTRACT_ID"].str.split("_", expand=True)  # noqa
            chunk["POLICY_NUMBER"] = split_contract[0]
            chunk["SEQUENCE_NUMBER"] = split_contract[1]
            chunk["PLAN_CODE"] = split_contract[2]

            # Log any rows with missing or malformed contract IDs
            invalid_mask = chunk["IFRS17_CONTRACT_ID"].isna() | (
                split_contract[2].isna()
            )
            invalid_contracts = chunk[invalid_mask]
            if not invalid_contracts.empty:
                logger.warning(
                    f"Found {len(invalid_contracts)} rows with invalid contract IDs"  # noqa
                )

            # add a col for product code
            chunk["PRODUCT_CODE"] = rpt_file.stem

            logger.info("Chunk manipulation complete")

            if is_first_chunk:
                # Get intersection of available columns and cols_to_keep
                output_columns = [col for col in cols_to_keep if col in chunk.columns]  # noqa
                output_columns = [
                    "POLICY_NUMBER",
                    "SEQUENCE_NUMBER",
                    "PLAN_CODE",
                    "PRODUCT_CODE",
                ] + output_columns
                logger.info(f"Selected {len(output_columns)} columns")
                logger.info(f"Columns: {output_columns}")

            # Write chunk to file
            write_chunked_csv(
                chunk,
                output_columns,
                out_file,
                mode="w" if is_first_chunk else "a",
                header=is_first_chunk,
            )

            logger.info(f"Wrote chunk to {out_file}")
            is_first_chunk = False  # Move this after writing

        end = time.perf_counter()
        logger.info(f"Processed {rpt_file.name} in {end - start:.2f}s")

    except Exception as e:
        logger.error(f"Error with {rpt_file}: {e}")
    finally:
        if local_copy.exists():
            local_copy.unlink()
