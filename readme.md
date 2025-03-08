# Booking.com Hotel Data Scraper

## Overview
This project is a collection of web scraping scripts designed to extract hotel policies, information, FAQs, comments, and images from **Booking.com**. The scraped data is processed, structured, and stored efficiently for further analysis using **Hadoop** and **PySpark**.

## Features
- **Web Scraping**: Extracts hotel-related information from Booking.com.
- **Asynchronous Processing**: Uses Playwright and Asyncio for efficient scraping.
- **HTML/XML Parsing**: Utilizes Lxml and Selectolax for structured data extraction.
- **Scalable Storage**: Stores scraped data in **Hadoop** for distributed storage.
- **Demo Big Data Processing**: Loads and processes data with **PySpark** for high-performance analysis.
- **Ethical Scraping**: Handles rate limits and dynamic content responsibly.

## Tech Stack
- **Web Scraping**: Playwright, Asyncio
- **Parsing & Data Processing**: Lxml, Selectolax, Pandas
- **Storage & Big Data**: Hadoop, PySpark

## Installation
Ensure you have the required dependencies installed:
```sh
pip install playwright asyncio lxml selectolax pandas pyspark
