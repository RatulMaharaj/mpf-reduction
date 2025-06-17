# zip up all the files ending with csv in the out_dir
import zipfile
from src.config import out_dir, is_omp

# Get all files in the out_data folder
files = list(out_dir.glob("*.csv"))


zip_file_name = "omp_runs.zip" if is_omp else "vantage_runs.zip"
zip_file_path = out_dir / zip_file_name

# Create a zip file with compression (ZIP_DEFLATED)
with zipfile.ZipFile(
    f"{zip_file_path}",
    "w",
    compression=zipfile.ZIP_DEFLATED,
) as zipf:
    for file in files:
        zipf.write(f"{zip_file_path}", file)
