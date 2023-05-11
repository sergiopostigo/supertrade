import pandas as pd
import datetime
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.spatial.distance import jensenshannon
from scipy import stats
import itertools


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




def jensen_shannon(*samples):

    # Get the max and min observation among all the data
    all_observations = [observation for sample in samples for observation in sample]
    max_observation = max(all_observations)
    min_observation = min(all_observations)

    # Define the bins
    n_bins = 10
    bins = np.linspace(min_observation, max_observation, n_bins+1)

    # Create the probability vector for each sample
    probability_vectors = []
    for sample in samples:
        # take the observations of the respective sample
        observations = np.array(sample)
        # count the ocurrences in each bin
        freq_count, bin_edges = np.histogram(observations, bins=bins)
        # calculate the probability of each bin
        probability_vector = freq_count/len(observations)
        # append to the probability vectors list
        probability_vectors.append(probability_vector)

    # Get the Jensen-Shannon Distances from all pairs
    pairs = itertools.combinations(probability_vectors, 2)
    jensen_shanon_distances = []
    for pair in pairs:
        jsd = jensenshannon(pair[0], pair[1],base=2)
        jensen_shanon_distances.append(jsd)

    # Get the Jensen-Shannon Distances from consecutive samples (time wise speaking)
    # jensen_shanon_distances = []
    # for i in range(len(probability_vectors) - 1):
    #     jsd = jensenshannon(probability_vectors[i], probability_vectors[i+1], 2)
    #     jensen_shanon_distances.append(jsd)

    return max(jensen_shanon_distances)

def time_granularity_analysis(cyclic_granularity, observations, data, distance_metric, plot, outliers, n_resamples=1000):

    # Measure seasonality
    # Get the observations from each subset from the time granularity (sorted by label in ascending order)
    samples = [data.loc[data[cyclic_granularity] == str(subset), observations].values
               for subset in sorted(map(int, data[cyclic_granularity].unique()))]
    # Remove samples with less than 2 observations (caused by leap years, from 2024 this won't be needed)
    samples = [sample for sample in samples if len(sample) >= 2]
    # Remove outliers
    #samples = [sample[(np.abs(sample - np.mean(sample)) < (1.5*np.std(sample)))] for sample in samples]

    # Permutation test (statistic: max Jensen Shannon Distance between pairs of subsets)
    res = stats.permutation_test(samples, distance_metric, n_resamples=n_resamples, alternative='greater')

    # PLOTS:
    # Distribution's plot
    if plot==True:
        fig, axs = plt.subplots(ncols=2, figsize=(18,6))
        sns.boxenplot(data=data,
                        x=cyclic_granularity,
                        y=observations,
                        order=[str(i) for i in data[cyclic_granularity].astype(int).sort_values().unique()],
                        k_depth=2,
                        showfliers = outliers,
                        palette=['#abaef3'],
                        ax=axs[0])
        axs[0].set_title('{} @ {}'.format(observations, cyclic_granularity))

        # Histogram with the statistics gotten from each permutation
        axs[1].hist(res.null_distribution, bins=100)
        axs[1].set_title("Permutation distribution of test statistic")
        axs[1].set_xlabel("Value of Statistic")
        axs[1].set_ylabel("Frequency")
        axs[1].axvline(res.statistic, color='red', linestyle='dashed', linewidth=2)
        axs[1].text(axs[1].get_xlim()[1]*0.97, axs[1].get_ylim()[1]*0.95, 'Statistic: {:.4f}\np-value: {:.4f}'.format(res.statistic, res.pvalue), ha='right', va='top', fontsize=12)
        plt.show()

    return {cyclic_granularity: {'statistic':res.statistic, 'pvalue':res.pvalue}}