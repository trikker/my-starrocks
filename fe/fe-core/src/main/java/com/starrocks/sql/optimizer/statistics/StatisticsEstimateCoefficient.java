// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.statistics;

public class StatisticsEstimateCoefficient {
    // Estimated parameters for multiple join on predicates when predicate correlation is not known
    public static final double UNKNOWN_AUXILIARY_FILTER_COEFFICIENT = 0.9;
    // Group by columns correlation in estimate aggregates row count
    public static final double UNKNOWN_GROUP_BY_CORRELATION_COEFFICIENT = 0.75;
    // estimate aggregates row count with default group by columns statistics
    public static final double DEFAULT_GROUP_BY_CORRELATION_COEFFICIENT = 0.5;
    // expand estimate aggregates row count with default group by columns statistics
    public static final double DEFAULT_GROUP_BY_EXPAND_COEFFICIENT = 1.05;
    // IN predicate default filter rate
    public static final double IN_PREDICATE_DEFAULT_FILTER_COEFFICIENT = 0.5;
    // Is null predicate default filter rate
    public static final double IS_NULL_PREDICATE_DEFAULT_FILTER_COEFFICIENT = 0.1;
    // unknown filter coefficient for now
    public static final double PREDICATE_UNKNOWN_FILTER_COEFFICIENT = 0.25;
    // constant value compare constant value filter coefficient
    public static final double CONSTANT_TO_CONSTANT_PREDICATE_COEFFICIENT = 0.5;
    // coefficient of overlap percent which overlap range is infinite
    public static final double OVERLAP_INFINITE_RANGE_FILTER_COEFFICIENT = 0.5;
    // used in compute extra cost for multi distinct function, estimate whether to trigger streaming
    public static final double STREAMING_EXTRA_COST_THRESHOLD_COEFFICIENT = 0.8;
    // default mysql external table output rows
    public static final int DEFAULT_MYSQL_OUTPUT_ROWS = 10000;
    // default es external table output rows
    public static final int DEFAULT_ES_OUTPUT_ROWS = 5000;
    // default JDBC external table output rows, JDBC maybe is a distribute system
    public static final int DEFAULT_JDBC_OUTPUT_ROWS = 20000;
    // if after aggregate row count < (input row count * DEFAULT_AGGREGATE_EFFECT_COEFFICIENT),
    // the aggregate has good effect.
    public static final double DEFAULT_AGGREGATE_EFFECT_COEFFICIENT = 0.001;
    // default selectivity for anti jion
    public static final double DEFAULT_ANTI_JOIN_SELECTIVITY_COEFFICIENT = 0.4;
}
