from pathlib import Path

input_dir = Path(
    R"\\OMRPRTP05.za.omlac.net\DEVELOPMENT\PE_Results\Segments\MFC RSA\2024-12\Mass Risk\Full"  # noqa
)
local_temp_dir = Path(R"C:\Temp\MPF_Files")
out_dir = Path(R"out")

use_local_copy = False

run_numbers = ["179", "250"]
runs_of_interest = [
    "309",
    "26",
    "28",
    "29",
    "301",
    "302",
    "303",
    "304",
    "305",
    "306",
    "307",
    "308",
    "310",
    "311",
    "312",
    "313",
    "314",
    "315",
    "316",
    "317",
    "318",
    "319",
    "320",
]  # noqa
