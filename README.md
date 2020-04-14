Toolset used to perform the analysis in Silver et al (2020) *Online crowdfunding maintains existing socioeconomic disparities in cancer care*.


### Installation

You will need Python 3 and R to run this package.

Anaconda users can use the included conda environment to assemble the Python 3 dependencies:

```conda env create -f dependencies.yml```


---

### Running the application

#### A. Gathering public data:

##### GoFundMe data
Our process to assemble the crowdfunding dataset in Silver et al (2020) can be replicated by running:

```python3 scrape.py```

This script gathers the campaigns included in GoFundMe.com's sitemap (https://www.gofundme.com/sitemap.xml). The resulting sample is complemented by the historical data stored on the Internet Archive (Archive.org) by querying the archive's API (https://archive.org/services/docs/api/).

Outputs will be stored in folders outside of the git repository. For cleaning, you will need to specify the path to the resulting scraped .csv files in ```src/data_io.py``` 

**Important note: some variation is expected in the resulting sample depending on when data are retrieved.**

##### Census data
American Community Survey (ACS) Census data for years 2013-2017 are collected by running ```notebooks/get_census_data.ipynb```. The Census variables to collect are specified in ```data/census/census_variables.csv```.

Note that you will need to add your own Census API key to ```src/tokens.py``` (see https://api.census.gov/data/key_signup.html).


#### B. Preparing the data for analysis:

##### GoFundMe data

  * After scraping the sitemap, add the resulting .csv files to the directory ```data_io.input_raw/gfm``` and run ```notebooks/Make master tables.ipynb```. This notebook excludes duplicate URLs and poor-quality scrapes from the Wayback Machine.

  * Once the master table (all campaigns) has been made, run ```notebooks/clean_master_dataset.ipynb```. This notebook excludes non-US campaigns, non-cancer campaigns (based on keyword search), campaigns with tag other than "Medical", campaigns with no year, and duplicate campaigns based on title, year, and location. This notebook also parses information into standardized formats (e.g., amount raised, contributors, number of likes, etc.) Finally, this notebook generates a spreadsheet of unique locations that will be geocoded.

##### Census data
  * After collecting and saving Census data, run the principal components analysis (PCA) in R using ```R/clustr.R```. This script runs a PCA in two steps, as described in Messer et al., 2006 (https://www.ncbi.nlm.nih.gov/pubmed/17031568). The PCA outputs are then used to calculate a Neighborhood Deprivation Index, which is split into quartiles in ```notebooks/assign locations and text indicators.ipynb``` (see below).

##### Location data
  * Once unique locations have been generated by ```notebooks/clean_master_dataset.ipynb```, you will use Mapbox's search API and the FCC's FIP search API to assign counties and FIP codes to all unique locations. You will need a Census Bureau API for the FCC requests, and a Mapbox API for the Mapbox requests. To run the geocoder, use the notebook ```notebooks/geocoder_notebook.ipynb```.

##### Final processing: text mining and location mapping
  * Finally, use ```notebooks/assign locations and text indicators.ipynb``` to assign FIP codes and mine campaign text features (and exclude any campaigns that failed to geocode). This will generate a geocoded dataset with boolean indicators for text features specified in ```data_io.input_cleaned/gfm/free_text_search_terms.csv```.

#### C. Running the analysis:

  * The notebook: ```notebooks/census_and_gfm_analysis.ipynb``` joins the Census data with the GFM data based on county FIP code. It generates visuals for the PCA to ensure that neighborhood deprivation indices are in the expected direction with raw Census data. The notebook produces outputs for both univariate and multivariate analyses, which are saved in ```data_io.output_analysis/```.

---

### Authors

* Han Q. Truong
* Elisabeth R. Silver
* Sassan Ostvar


### References
