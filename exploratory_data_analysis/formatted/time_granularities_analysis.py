"""
Formatted Zone
Time Granularities Analysis

Classes and functions for the time granularity analysis, that assesses the level of similarity of distributions
across a cyclic granularity. If the distributions are different enough, this is means that there is a seasonal
pattern in the time-series determined by the cyclic granularity.

"""


import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from scipy.spatial.distance import jensenshannon
from scipy import stats
import itertools

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

class time_granularity_analysis:
    """Time granularity analysis engine

    Develop a similarity test between the distributions of a cyclic granularity.

    Attributes:
        data : dataframe
            table with time-series and the associated cyclic granularity labels
        cyclic_granularity: string
            Cyclic granularity column
        observations :string
            Observations (time-series) column

    Methods:
        distributions_similarity_test()
            Response of the permutation test
        summary()
            Gives the statistic value and the p-value of the permutation test
        plot()
            Plot the distributions at every level of the granularity's categories and also the histogram
            with the statistics after every permutation. Also shows the observed statistic.
    """
    def __init__(self, data, cyclic_granularity, observations):
        self.data = data
        self.cyclic_granularity = cyclic_granularity
        self.observations = observations

    def distributions_similarity_test(self):

        # Measure seasonality
        # Get the observations from each subset from the time granularity (sorted by label in ascending order)
        samples = [self.data.loc[self.data[self.cyclic_granularity] == str(subset), self.observations].values
                   for subset in sorted(map(int, self.data[self.cyclic_granularity].unique()))]
        # Remove samples with less than 2 observations (caused by leap years, from 2024 this won't be needed)
        samples = [sample for sample in samples if len(sample) >= 2]
        # Remove outliers
        #samples = [sample[(np.abs(sample - np.mean(sample)) < (1.5*np.std(sample)))] for sample in samples]

        # Permutation test (statistic: max Jensen Shannon Distance between pairs of subsets)
        res = stats.permutation_test(samples, jensen_shannon, n_resamples=1000, alternative='greater')

        return res

    def summary(self):
        res = self.distributions_similarity_test()
        return {self.cyclic_granularity: {'statistic': res.statistic,
                                          'pvalue': res.pvalue}}

    def plot(self, outliers=False):

        res = self.distributions_similarity_test()
        # Distribution's plot
        fig, axs = plt.subplots(ncols=2, figsize=(18,6))
        sns.boxenplot(data=self.data,
                        x=self.cyclic_granularity,
                        y=self.observations,
                        order=[str(i) for i in self.data[self.cyclic_granularity].astype(int).sort_values().unique()],
                        k_depth=2,
                        showfliers = outliers,
                        palette=['#B20E0F'],
                        ax=axs[0])
        axs[0].set_title('{} @ {}'.format(self.observations, self.cyclic_granularity))

        # Histogram with the statistics gotten from each permutation
        axs[1].hist(res.null_distribution, bins=100, color='#B20E0F')
        axs[1].set_title("Permutation distribution of test statistic")
        axs[1].set_xlabel("Value of Statistic")
        axs[1].set_ylabel("Frequency")
        axs[1].axvline(res.statistic, color='black', linestyle='dashed', linewidth=2)
        axs[1].text(axs[1].get_xlim()[1]*0.97, axs[1].get_ylim()[1]*0.95, 'Statistic: {:.4f}\np-value: {:.4f}'.format(res.statistic, res.pvalue), ha='right', va='top', fontsize=12)
        plt.show()

