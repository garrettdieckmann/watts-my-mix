# Scrape EIA930 API
Existing data loaders in this project rely upon the EIA's 6-month CSV files. In order to get more up-to-date data, can use this scraper which will collect data for a given Balancing Authority.

## Usage
To collect EIA 930 generation data starting from December 20th, 2022 for Seattle City Light, run the following:
```
$ EIA_API_KEY=<your EIA key>
$ MONGO_CONNECTION_STRING=<your mongodb connection string>
$ node dist/index.js -ba SCL -f 2022-12-20T00 -d generation
```

## Help
```
$ node dist/index.js --help
Usage: index [options]

CLI script to scrape EIA 930 data

Options:
  -V, --version                       output the version number
  -ba, --balancing-authority <value>  Balancing Authority to scrape for
  -f, --from-time <datetime>          Earliest datetime (format: '2022-12-13T00') to pull
  -t, --to-time <datetime>            Latest datetime (format: '2022-12-13T24') to pull
  -d, --data-set <value>              Which dataset to retrieve. Allowed: ['generation', 'interchange'] (default: "generation")
  -h, --help                          display help for command
```