# zip up all the files ending with csv in the out_dir
import zipfile
from src.config import out_dir

# Get all combined files in the out_dir folder
files = list(out_dir.glob("combined_*.csv"))

# Create a zip file for each combined file
for file in files:
    # Create zip filename based on the original file name
    zip_file_name = f"{file.stem}.zip"
    zip_file_path = out_dir / zip_file_name

    # Create a zip file with compression (ZIP_DEFLATED)
    with zipfile.ZipFile(
        f"{zip_file_path}",
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as zipf:
        zipf.write(f"{zip_file_path}", file.name)
