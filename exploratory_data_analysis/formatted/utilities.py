import pandas as pd
import datetime
import numpy as np


def generate_time_granularities(start_date, end_date):

    """Return time granularities

    Given two dates, return the cyclic granularities:

    year, semester_in_year, quarter_in_year, month_in_year, month_in_semester, month_in_quarter, quarter_in_semester,
    day_in_year, day_in_semester, day_in_quarter, day_in_month, day_in_week, week_in_year, week_in_semester,
    week_in_quarter, week_in_month

    Args:
        start_date (datetime): Start date
        end_date (datetime): End date

    Returns:
        time_granularities (dataframe): dataframe with time granularities
    """

    # Generate the date index range with day frequency
    time_granularities = pd.date_range(start=start_date, end=end_date, freq='D')
    time_granularities = pd.DataFrame(index=time_granularities)

    # Create the cyclic time granularities

    # Circular granularities
    time_granularities['year'] = time_granularities.index.year
    time_granularities['semester_in_year'] = np.ceil(time_granularities.index.month / 6).astype(np.int64)
    time_granularities['quarter_in_year'] = np.ceil(time_granularities.index.month / 3).astype(np.int64)
    time_granularities['month_in_year'] = time_granularities.index.month
    time_granularities['month_in_semester'] = [(i % 6 if i % 6 > 0 else 6) for i in time_granularities.index.month]
    time_granularities['month_in_quarter'] = [(i % 3 if i % 3 > 0 else 3) for i in time_granularities.index.month]
    time_granularities['quarter_in_semester'] = [(i % 2 if i % 2 > 0 else 2) for i in time_granularities.index.month]

    # Quasi-circular granularities
    time_granularities['day_in_year'] = time_granularities.index.day_of_year
    time_granularities['day_in_semester'] = day_in_semester(start_date,end_date)
    time_granularities['day_in_quarter'] = day_in_quarter(start_date,end_date)
    time_granularities['day_in_month'] = time_granularities.index.day
    time_granularities['day_in_week'] = time_granularities.index.day_of_week + 1
    time_granularities['week_in_year'] = week_in_year(start_date, end_date)
    time_granularities['week_in_semester'] = week_in_semester(start_date, end_date)
    time_granularities['week_in_quarter'] = week_in_quarter(start_date, end_date)
    time_granularities['week_in_month'] = week_in_month(start_date, end_date)

    time_granularities = time_granularities.astype(str)

    return time_granularities

def week_in_year(start_date, end_date):

    # Create the dataframe that includes all the dates of the corresponding years
    df = pd.date_range(start=start_date.replace(day=1, month=1), end=end_date.replace(day=31, month=12), freq='D')
    df = pd.DataFrame(index=df)
    # Define the needed
    df['year'] = df.index.year
    df['day_in_week'] = df.index.weekday + 1

    # Generate the counts
    week_count = []
    for year in df['year'].unique().tolist():
        count = 1
        for i in df[df['year'] == year]['day_in_week'].tolist():
            if i == 7:
                week_count.append(count)
                count = count + 1
            else:
                week_count.append(count)
    df['corrected_day_in_week'] = week_count

    return df[df.index.isin(pd.date_range(start=start_date, end=end_date, freq='D'))]['corrected_day_in_week'].tolist()

def week_in_month(start_date, end_date):

    # Create the dataframe that includes all the dates of the corresponding years
    df = pd.date_range(start=start_date.replace(day=1, month=1), end=end_date.replace(day=31, month=12), freq='D')
    df = pd.DataFrame(index=df)
    # Define the needed
    df['year'] = df.index.year
    df['month_in_year'] = df.index.month
    df['day_in_week'] = df.index.weekday + 1

    # Generate the counts
    week_count = []
    for year in df['year'].unique().tolist():
        for month in df['month_in_year'].unique().tolist():
            count = 1
            for i in df[(df['year'] == year) & (df['month_in_year'] == month)]['day_in_week'].tolist():
                if i == 7:
                    week_count.append(count)
                    count = count + 1
                else:
                    week_count.append(count)
    df['week_in_month'] = week_count

    return df[df.index.isin(pd.date_range(start=start_date, end=end_date, freq='D'))]['week_in_month'].tolist()

def week_in_semester(start_date, end_date):

    # Create the dataframe that includes all the dates of the corresponding years
    df = pd.date_range(start=start_date.replace(day=1, month=1), end=end_date.replace(day=31, month=12), freq='D')
    df = pd.DataFrame(index=df)
    # Define the needed
    df['year'] = df.index.year
    df['semester_in_year'] = np.ceil(df.index.month / 6).astype(np.int64)
    df['day_in_week'] = df.index.weekday + 1

    # Generate the counts
    week_count = []
    for year in df['year'].unique().tolist():
        for quarter in df['semester_in_year'].unique().tolist():
            count = 1
            for i in df[(df['year'] == year) & (df['semester_in_year'] == quarter)]['day_in_week'].tolist():
                if i == 7:
                    week_count.append(count)
                    count = count + 1
                else:
                    week_count.append(count)
    df['week_in_semester'] = week_count

    return df[df.index.isin(pd.date_range(start=start_date, end=end_date, freq='D'))]['week_in_semester'].tolist()

def week_in_quarter(start_date, end_date):

    # Create the dataframe that includes all the dates of the corresponding years
    df = pd.date_range(start=start_date.replace(day=1, month=1), end=end_date.replace(day=31, month=12), freq='D')
    df = pd.DataFrame(index=df)
    # Define the needed
    df['year'] = df.index.year
    df['quarter_in_year'] = np.ceil(df.index.month / 3).astype(np.int64)
    df['day_in_week'] = df.index.weekday + 1

    # Generate the counts
    week_count = []
    for year in df['year'].unique().tolist():
        for quarter in df['quarter_in_year'].unique().tolist():
            count = 1
            for i in df[(df['year'] == year) & (df['quarter_in_year'] == quarter)]['day_in_week'].tolist():
                if i == 7:
                    week_count.append(count)
                    count = count + 1
                else:
                    week_count.append(count)
    df['week_in_quarter'] = week_count

    return df[df.index.isin(pd.date_range(start=start_date, end=end_date, freq='D'))]['week_in_quarter'].tolist()

def day_in_semester(start_date, end_date):

    # Create the dataframe that includes all the dates of the corresponding years
    df = pd.date_range(start=start_date.replace(day=1, month=1), end=end_date.replace(day=31, month=12), freq='D')
    df = pd.DataFrame(index=df)
    # Define the needed
    df['year'] = df.index.year
    df['semester_in_year'] = np.ceil(df.index.month / 6).astype(np.int64)
    df['day_in_year'] = df.index.day_of_year

    # Generate the counts
    day_count = []
    for year in df['year'].unique().tolist():
        for semester in df['semester_in_year'].unique().tolist():
            count = 1
            for i in df[(df['year'] == year) & (df['semester_in_year'] == semester)]['day_in_year'].tolist():
                day_count.append(count)
                count = count + 1
    df['day_in_semester'] = day_count

    return df[df.index.isin(pd.date_range(start=start_date, end=end_date, freq='D'))]['day_in_semester'].tolist()

def day_in_quarter(start_date, end_date):

    # Create the dataframe that includes all the dates of the corresponding years
    df = pd.date_range(start=start_date.replace(day=1, month=1), end=end_date.replace(day=31, month=12), freq='D')
    df = pd.DataFrame(index=df)
    # Define the needed
    df['year'] = df.index.year
    df['quarter_in_year'] = np.ceil(df.index.month / 3).astype(np.int64)
    df['day_in_year'] = df.index.day_of_year

    # Generate the counts
    day_count = []
    for year in df['year'].unique().tolist():
        for quarter in df['quarter_in_year'].unique().tolist():
            count = 1
            for i in df[(df['year'] == year) & (df['quarter_in_year'] == quarter)]['day_in_year'].tolist():
                day_count.append(count)
                count = count + 1
    df['day_in_quarter'] = day_count

    return df[df.index.isin(pd.date_range(start=start_date, end=end_date, freq='D'))]['day_in_quarter'].tolist()