# Stock Portfolio Builder

A financial web application written in Flask that allows the creation of dummy portfolios for US-based stocks.

* Front-End: HTML, CSS, Javascript, Plotly
* Back-End: Flask, APScheduler, SQLAlchemy, MySQL
* Data Cleaning Libraries: Pandas

# Table of Contents

- [Overview](https://github.com/will-i-amv/portfolio-builder-flask#Overview)
- [How it Works](https://github.com/will-i-amv/portfolio-builder-flask#Installation)
- [Features](https://github.com/will-i-amv/portfolio-builder-flask#Features)
- [Credits](https://github.com/will-i-amv/portfolio-builder-flask#Credits)
- [Disclaimer](https://github.com/will-i-amv/portfolio-builder-flask#Disclaimer)
- [License](https://github.com/will-i-amv/portfolio-builder-flask#License)

# Overview

The web app allows users to create an unlimited number of dummy portfolios to test their strategies, monitor potential performance, or just track certain sectors they are interested in. For now only stocks from US exchanges (NYSE and NASDAQ) are available.

# How it Works

* After registering and logging in, the user can create any number of portfolios.
* Then the user can start adding initial positions for each security to a portfolio, specifying the details of each position. 
* The user can update a position by adding buy and sell operations.
* The site will display a dashboard with various metrics based on the information entered by the user. 
* If there's a new security that doesn't have prices stored in the database, an API call will be made to get daily prices for the last 100 days. 
* Daily prices for all existing securities are updated at 1am every day.

# Features

The following features are implemented:
* A portfolio page that allow CRUD operations on the available portfolios.
* A dashboard to visualize the following portfolio metrics:
    - FIFO accounting applied to the positions of the portfolio.
    - Daily Holding Period Return of the portfolio.
    - Pie and Bar charts to help visualise the portfolio composition.

# Credits

A big thank you to the FLask, SQLAlchemy and Pandas communities for giving us such high quality libraries on which this project is built.

# Disclaimer

This software is for educational purposes only. USE THE SOFTWARE AT YOUR OWN RISK. I ASSUME NO RESPONSIBILITY FOR YOUR TRADING RESULTS. Do not risk money that you can't afford to lose. There might be bugs in the code - this software DOES NOT come with ANY warranty.

# Licence

MIT Licence.
