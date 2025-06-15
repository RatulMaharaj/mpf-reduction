from pathlib import Path

input_dir = Path(
    R"\\OMRPRTP01.za.omlac.net\PRODUCTION\PE_Results\OM1\MFC RSA\2024-12\Mass Protect"  # noqa
)
local_temp_dir = Path(R"C:\Temp\MPF_Files")
out_dir = Path(
    R"\\OMRPRTP05.za.omlac.net\DEVELOPMENT\PE_Results\Segments\MFC RSA\2024-12\Mass Risk\Ratul\OMP"  # noqa
)
is_omp = True
use_local_copy = True

run_numbers = ["179", "250"]
runs_of_interest = [
    "0",
    "408",
    "413",
    "414",
    "418",
    "419",
    "420",
    "421",
    "422",
    "423",
    "415",
    "424",
    "425",
    "426",
    "427",
    "428",
    "429",
    "430",
    "431",
    "432",
    "433",
]  # noqa
