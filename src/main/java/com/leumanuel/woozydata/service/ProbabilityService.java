package com.leumanuel.woozydata.service;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.PoissonDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

/**
 * Service class for probability distributions and statistical calculations.
 * Provides methods for generating various probability distributions and calculating probabilities.
 *
 * @author Leu A. Manuel
 * @version 1.0
 */
public class ProbabilityService {
 
    /**
     * Generates samples from a normal distribution.
     *
     * @param size Number of samples to generate
     * @param mean Mean of the distribution
     * @param stdDev Standard deviation of the distribution
     * @return Array of samples from normal distribution
     */
    public double[] generateNormalDistribution(int size, double mean, double stdDev) {
        NormalDistribution distribution = new NormalDistribution(mean, stdDev);
        double[] samples = new double[size];
        for (int i = 0; i < size; i++) {
            samples[i] = distribution.sample();
        }
        return samples;
    }

    /**
     * Generates samples from a binomial distribution.
     *
     * @param trials Number of trials
     * @param probability Success probability
     * @param size Number of samples to generate
     * @return Array of samples from binomial distribution
     */
    public int[] generateBinomialDistribution(int trials, double probability, int size) {
        BinomialDistribution distribution = new BinomialDistribution(trials, probability);
        int[] samples = new int[size];
        for (int i = 0; i < size; i++) {
            samples[i] = distribution.sample();
        }
        return samples;
    }

    /**
     * Calculates the probability density function for normal distribution.
     *
     * @param x Value to calculate PDF for
     * @param mean Mean of the distribution
     * @param stdDev Standard deviation of the distribution
     * @return PDF value
     */
    public double normalPDF(double x, double mean, double stdDev) {
        NormalDistribution distribution = new NormalDistribution(mean, stdDev);
        return distribution.density(x);
    }
    
    /**
     * Calculates the cumulative distribution function for normal distribution.
     *
     * @param x Value to calculate CDF for
     * @param mean Mean of the distribution
     * @param std Standard deviation of the distribution
     * @return CDF value
     */
    public double normalCDF(double x, double mean, double std) {
        NormalDistribution distribution = new NormalDistribution(mean, std);
        return distribution.cumulativeProbability(x);
    }

    /**
     * Generates samples from a Poisson distribution.
     *
     * @param lambda Rate parameter
     * @param size Number of samples to generate
     * @return Array of samples from Poisson distribution
     */
    public double[] generatePoissonDistribution(double lambda, int size) {
        PoissonDistribution distribution = new PoissonDistribution(lambda);
        double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            result[i] = distribution.sample();
        }
        return result;
    }

    /**
     * Generates samples from a uniform distribution.
     *
     * @param size Number of samples to generate
     * @param min Minimum value
     * @param max Maximum value
     * @return Array of samples from uniform distribution
     */
    public double[] generateUniformDistribution(int size, double min, double max) {
        UniformRealDistribution distribution = new UniformRealDistribution(min, max);
        double[] result = new double[size];
        for (int i = 0; i < size; i++) {
            result[i] = distribution.sample();
        }
        return result;
    }
}
