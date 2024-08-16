# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

## [0.1.0] - 2024-08-12
### Added
- Initial project setup
- README.md file
- CHANGELOG.md file
- read_csv.ipynb file to read the GDELT data

### Findings

1. The datasets (gkg, events) do not contain text news, only URLs. Should we scrape the news from the URLs?
2. I couldn't find a link between the gkg and events datasets. how do we know which news is related to which event?