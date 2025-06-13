import pandas as pd
import csv
from io import StringIO
from pathlib import Path
from typing import List
import logging

logger = logging.getLogger(__name__)


def textfile_to_filtered_str_list(
    file_path: str | Path, header_prefix: str, entries_prefix: str, sep: str
) -> List[str]:
    """Converts a textfile to a filtered list of strings

    Parameters
    ----------
    file_path : str | Path
         Path object or string path
    header_prefix : str
        Header marker
    entries_prefix : str
        Entry marker
    sep: str
        Delimiter to use

    Returns
    -------
    List[str]
        Each entry represents a line from the text file
    """
    # initialize results
    result = []
    # keep track of whether or not the header row was processed
    found_header = False

    with open(file_path) as file:
        for _, line in enumerate(file):
            if not line:
                pass  # do nothing if row is empty
            elif (not found_header) and (line.split(sep)[0] == header_prefix):
                found_header = True
                result.append(line)
            elif found_header and (line.split(sep)[0] == entries_prefix):
                result.append(line)

    return result


def str_list_to_stringIO(str_list: List[str], newline="\r\n") -> StringIO:
    """Converts a list of strings to one continuous string, with linebreaks;
    and then wraps that in a stringIO.

    Parameters
    ----------
    str_list : List[str]
        List of strings
    newline : str, optional
        New line character, by default "\r\n"

    Returns
    -------
    StringIO
        StringIO object to be consumed by read_csv
    """
    big_string = newline.join(str_list)
    str_io = StringIO(big_string)
    return str_io


def read_rpt(
    file_path: str | Path,
    header_prefix="!",
    entries_prefix="*",
    sep=",",
    chunksize: int | None = None,
    **csv_args,
) -> pd.DataFrame | pd.io.parsers.TextFileReader:
    """Read a .rpt file into a pandas DataFrame or TextFileReader for chunked reading.

    This is a special text file format where the header row begins with a "!"
    and each data row begins with a "*".

    Parameters
    ----------
    file_path : str
        Path object or string path
    header_prefix : str, optional
        Header marker, by default "!"
    entries_prefix : str, optional
        Entry marker, by default "*"
    sep: str
        Delimiter to use
    chunksize : int | None, optional
        Number of rows to read at a time. If None, reads entire file at once.

    Returns
    -------
    pd.DataFrame | pd.io.parsers.TextFileReader
        Returns either a pandas DataFrame object or a TextFileReader for chunked reading
    """  # noqa
    str_list = textfile_to_filtered_str_list(
        file_path, header_prefix, entries_prefix, sep
    )  # reads header (!) and entries (*) only

    str_io = str_list_to_stringIO(
        str_list
    )  # converts list of strings to string_IO needed by pd.read_csv

    if chunksize is None:
        df = pd.read_csv(str_io, sep=sep, **csv_args).drop(header_prefix, axis=1)  # noqa
        return df
    else:
        return pd.read_csv(str_io, sep=sep, chunksize=chunksize, **csv_args)


def textfile_to_str_list(file_path):
    result = []
    with open(file_path) as file:
        result = [line.strip() for line in file]
    return result


def str_List_to_textfile(str_list):
    blob = "\n".join(str_list)
    return blob


def quote_all_strings(
    x, quote='"'
):  # puts quotes around strings and returns other values unchanged
    return quote + x + quote if isinstance(x, str) else x


# The following extension allows us to do -> df.rpt.to_rpt("file_name.rpt")
# without needing to subclass pd.DataFrame
@pd.api.extensions.register_dataframe_accessor("rpt")
class PredictableAccessor:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def to_rpt(self, file_name: str | None = None, sort_on="SPCODE"):
        # Wrap all strings in quotes
        df = self._obj.applymap(quote_all_strings)

        # Add ! heading with all * entries
        df.insert(0, "!", "*")

        # Sort if required
        if sort_on in list(df.columns):
            df = df.sort_values(sort_on)

        # write to file if file_name is provided
        if file_name:
            return df.to_csv(file_name, index=False, quoting=csv.QUOTE_NONE)
        else:
            return df


def write_chunked_csv(
    df_chunk: pd.DataFrame,
    output_columns: list[str],
    out_file: Path,
    mode: str = "w",
    header: bool = True,
):  # noqa
    """Write a chunk of data to a CSV file.

    Parameters
    ----------
    df_chunk : pd.DataFrame
        Chunk of data to write
    output_columns : list[str]
        List of columns to write
    out_file : Path
        Path to output file
    mode : str, optional
        File write mode, by default 'w'
    header : bool, optional
        Whether to write header, by default True
    """
    # Select only the columns we want
    df_to_write = df_chunk[output_columns]

    # Write to CSV with specific settings to prevent triple quoting
    df_to_write.to_csv(
        out_file,
        mode=mode,
        header=header,
        index=False,
        quoting=csv.QUOTE_MINIMAL,  # Only quote when necessary
        escapechar="\\",  # Use backslash as escape character
    )


def stream_rpt_file(file_path: str | Path, chunk_size: int = 10_000):
    """Stream process an RPT file line by line and yield chunks of data.

    Parameters
    ----------
    file_path : str | Path
        Path to the RPT file
    chunk_size : int, optional
        Number of lines to process in each chunk, by default 10000

    Yields
    ------
    pd.DataFrame
        Chunks of data as pandas DataFrames
    """
    header = None
    chunk = []
    count = 0
    total_rows = 0

    try:
        with open(file_path, "r") as file:
            for line_num, line in enumerate(file, 1):
                if not line.strip():
                    continue

                try:
                    parts = line.strip().split(",")

                    # Process header
                    if parts[0] == "!":
                        # Remove any quotes from header
                        header = [col.strip('"') for col in parts[1:]]
                        msg = f"Found header with {len(header)} columns: {header}"  # noqa
                        logger.info(msg)
                        continue

                    # Process data rows
                    if parts[0] == "*":
                        if len(parts[1:]) != len(header):
                            msg = (
                                f"Line {line_num}: Row has {len(parts[1:])} "
                                f"columns but header has {len(header)} columns"
                            )
                            logger.warning(msg)
                            continue

                        # Clean up the data values
                        cleaned_parts = [val.strip('"') for val in parts[1:]]
                        chunk.append(cleaned_parts)
                        count += 1
                        total_rows += 1

                        # Yield chunk when it reaches the desired size
                        if count >= chunk_size:
                            df = pd.DataFrame(chunk, columns=header)
                            msg = (
                                f"Yielding chunk of {len(df)} rows "
                                f"(total processed: {total_rows})"
                            )
                            logger.info(msg)
                            yield df
                            chunk = []
                            count = 0
                except Exception as e:
                    logger.error(f"Error processing line {line_num}: {str(e)}")
                    continue

        # Yield any remaining data
        if chunk:
            df = pd.DataFrame(chunk, columns=header)
            msg = (
                f"Yielding final chunk of {len(df)} rows "
                f"(total processed: {total_rows})"
            )
            logger.info(msg)
            yield df

    except Exception as e:
        logger.error(f"Error in stream_rpt_file {file_path}: {str(e)}")
        raise
