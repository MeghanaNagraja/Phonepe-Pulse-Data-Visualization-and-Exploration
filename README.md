# Problem Statement:

The Phonepe pulse Github repository contains a large amount of data related to
various metrics and statistics. The goal is to extract this data and process it to obtain
insights and information that can be visualized in a user-friendly manner.

![img1](https://github.com/MeghanaNagraja/Phonepe-Pulse-Data-Visualization-and-Exploration/assets/122547199/39c6b689-7c9f-4617-a7b3-b56f163a93ee)

# Approach:

    - Cloning data from PhonePe Github repository
    - Extracting data in json format
    - Uploading to SQL
    - Creating streamlit dashboard
    - Getting insights from data and visualisation

# Requirements:
    - Pandas
    - Streamlit
    - SQLAlchemy
    - Git
    - Plotly

# Cloning data from PhonePe Pulse github repository:

    -git clone https://github.com/PhonePe/pulse.git 
    -above command passed from terminal and data is saved to local project repository

# Extracting data and migrating to SQL database:

    - Data is in following folder structure
    - Statewise data is extracted for every year and quarter
    - Data is converted to pandas dataframe and uploaded to MySQL

![image_1](https://github.com/MeghanaNagraja/Phonepe-Pulse-Data-Visualization-and-Exploration/assets/122547199/96ae24cb-756e-4cbc-ab62-ba0bb7bb8933)

# Streamlit dashboard with data insights:

    - Top phone brands
    - Geomapping statewise overall transaction
    - Top 10 transactions districtwise
    - Growth in users
    - Least transaction states etc
