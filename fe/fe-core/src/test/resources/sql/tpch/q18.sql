[result]
TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
    TOP-N (order by [[13: O_TOTALPRICE DESC NULLS LAST, 14: O_ORDERDATE ASC NULLS FIRST]])
        AGGREGATE ([GLOBAL] aggregate [{56: sum=sum(24: L_QUANTITY)}] group by [[2: C_NAME, 1: C_CUSTKEY, 10: O_ORDERKEY, 14: O_ORDERDATE, 13: O_TOTALPRICE]] having [null]
            INNER JOIN (join-predicate [1: C_CUSTKEY = 11: O_CUSTKEY] post-join-predicate [null])
                SCAN (columns[1: C_CUSTKEY, 2: C_NAME] predicate[null])
                EXCHANGE SHUFFLE[11]
                    INNER JOIN (join-predicate [10: O_ORDERKEY = 20: L_ORDERKEY] post-join-predicate [null])
                        LEFT SEMI JOIN (join-predicate [10: O_ORDERKEY = 37: L_ORDERKEY] post-join-predicate [null])
                            SCAN (columns[10: O_ORDERKEY, 11: O_CUSTKEY, 13: O_TOTALPRICE, 14: O_ORDERDATE] predicate[null])
                            EXCHANGE SHUFFLE[37]
                                AGGREGATE ([GLOBAL] aggregate [{54: sum=sum(41: L_QUANTITY)}] group by [[37: L_ORDERKEY]] having [54: sum > 315]
                                    SCAN (columns[37: L_ORDERKEY, 41: L_QUANTITY] predicate[null])
                        EXCHANGE SHUFFLE[20]
                            SCAN (columns[20: L_ORDERKEY, 24: L_QUANTITY] predicate[null])
[end]

