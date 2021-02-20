from pathlib import Path

gfm = Path.cwd()/"data" # master folder to store all inputs and outputs
# Move scraped .csv files to input_raw directory
input_raw = gfm / "raw_data"
input_cleaned = gfm / "data"
output = gfm / "output"
output_plots = output / "output_plots"
output_analysis = output / "output_analysis_results"
