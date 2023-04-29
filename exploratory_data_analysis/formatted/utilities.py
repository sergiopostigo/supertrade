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
    time_granularities['week_in_year'] = time_granularities.index.isocalendar().week
    time_granularities['week_in_semester'] = week_in_semester(start_date, end_date)
    time_granularities['week_in_quarter'] = week_in_quarter(start_date, end_date)
    time_granularities['week_in_month'] = week_in_month(start_date, end_date)

    time_granularities = time_granularities.astype(str)

    return time_granularities

## FOR DAYS COUNT GENERATIONS

def is_first_day_of_semester(date):
    if date.month == 1 and date.day == 1:
        return 'first_day_of_period'
    elif date.month == 7 and date.day == 1:
        return 'first_day_of_period'
    else: return 0

def is_first_day_of_quarter(date):
    if date.month == 1 and date.day == 1:
        return 'first_day_of_period'
    elif date.month == 4 and date.day == 1:
        return 'first_day_of_period'
    elif date.month == 7 and date.day == 1:
        return 'first_day_of_period'
    elif date.month == 10 and date.day == 1:
        return 'first_day_of_period'
    else: return 0

def day_count_generator(lst):
    new_lst = lst
    day_count =  1
    for i, val in enumerate(new_lst):
        if val == 'first_day_of_period':
            day_count = 1
            new_lst[i] = day_count
        else:
            day_count = day_count + 1
            new_lst[i] = day_count
    return new_lst

def day_in_semester(start_date, end_date):

    # Create the dataframe with date index (extending the starting date to the first day of the corresponding year)
    df = pd.date_range(start=start_date.replace(day=1, month=1), end=end_date, freq='D')
    df = pd.DataFrame(index=df)

    # Generate the day count
    df['day_in_semester'] = day_count_generator([is_first_day_of_semester(i) for i in df.index])

    # Keep only dates from start_date to end_date
    df = df.loc[start_date:end_date]

    return df

def day_in_quarter(start_date, end_date):

    # Create the dataframe with date index (extending the starting date to the first day of the corresponding year)
    df = pd.date_range(start=start_date.replace(day=1, month=1), end=end_date, freq='D')
    df = pd.DataFrame(index=df)

    # Generate the day count
    df['day_in_quarter'] = day_count_generator([is_first_day_of_quarter(i) for i in df.index])

    # Keep only dates from start_date to end_date
    df = df.loc[start_date:end_date]

    return df


## FOR WEEKS COUNT GENERATIONS
def is_first_thursday_of_month(date):
    if date.weekday() == 3 and date.day <= 7:
        return 'first_thursday'
    else: return 0
#%%
def is_first_thursday_of_quarter(date):
    if date.month ==1 and date.weekday() == 3 and date.day <= 7:
        return 'first_thursday'
    elif date.month ==4 and date.weekday() == 3 and date.day <= 7:
        return 'first_thursday'
    elif date.month ==7 and date.weekday() == 3 and date.day <= 7:
        return 'first_thursday'
    elif date.month ==10 and date.weekday() == 3 and date.day <= 7:
        return 'first_thursday'
    else: return 0
#%%
def is_first_thursday_of_semester(date):
    if date.month ==1 and date.weekday() == 3 and date.day <= 7:
        return 'first_thursday'
    elif date.month ==7 and date.weekday() == 3 and date.day <= 7:
        return 'first_thursday'
    else: return 0

def week_count_generator(lst):
    new_lst = lst
    # First fill the first weeks
    for i, val in enumerate(lst):
     if val=='first_thursday':
        # Set first thursday as 1
        new_lst[i]=1
        # Set previous 3 days as 1
        for j in range(max(0, i-3), i):
            new_lst[j] = 1
        # Set next 3 days as 1
        for j in range(i+1, min(len(lst), i+4)):
            new_lst[j] = 1
    # Now fill the other weeks
    for i, val in enumerate(new_lst):
        if val == 1:
            week = 2
            weekday = 0
            continue
        else:
            # Get current week
            weekday = weekday + 1
            if weekday < 7:
                new_lst[i] = week
            elif weekday == 7:
                new_lst[i] = week
                week = week + 1
                weekday = 0
    return new_lst

def week_in_month(start_date, end_date):

    # Get the most recent year whose first day was a thursday
    corrected_start_date = start_date.replace(day=1, month=1)
    while ((corrected_start_date.weekday() + 1) != 1):
        corrected_start_date = corrected_start_date.replace(year=(corrected_start_date.year - 1))

    # Dataframe with a date index in daily frequency
    df = pd.date_range(start=corrected_start_date, end=end_date, freq='D')
    df = pd.DataFrame(index=df)

    # Generate the week count
    df['week_in_month'] = week_count_generator([is_first_thursday_of_month(i) for i in df.index])

    # Keep only dates from start_date to end_date
    df = df.loc[start_date:end_date]

    return df

def week_in_semester(start_date, end_date):

    # Get the most recent year whose first day was a thursday
    corrected_start_date = start_date.replace(day=1, month=1)
    while ((corrected_start_date.weekday() + 1) != 1):
        corrected_start_date = corrected_start_date.replace(year=(corrected_start_date.year - 1))

    # Dataframe with a date index in daily frequency
    df = pd.date_range(start=corrected_start_date, end=end_date, freq='D')
    df = pd.DataFrame(index=df)

    # Generate the week count
    df['week_in_semester'] = week_count_generator([is_first_thursday_of_semester(i) for i in df.index])

    # Keep only dates from start_date to end_date
    df = df.loc[start_date:end_date]

    return df

def week_in_quarter(start_date, end_date):

    # Get the most recent year whose first day was a thursday
    corrected_start_date = start_date.replace(day=1, month=1)
    while ((corrected_start_date.weekday() + 1) != 1):
        corrected_start_date = corrected_start_date.replace(year=(corrected_start_date.year - 1))

    # Dataframe with a date index in daily frequency
    df = pd.date_range(start=corrected_start_date, end=end_date, freq='D')
    df = pd.DataFrame(index=df)

    # Generate the week count
    df['week_in_quarter'] = week_count_generator([is_first_thursday_of_quarter(i) for i in df.index])

    # Keep only dates from start_date to end_date
    df = df.loc[start_date:end_date]

    return df
