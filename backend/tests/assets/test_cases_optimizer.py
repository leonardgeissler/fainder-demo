from typing import TypedDict

from lark import ParseTree, Token, Tree


class CaseOpt(TypedDict):
    input_parse_tree: ParseTree
    expected_kw_merge_without_sorting: ParseTree
    expected_kw_merge_with_sorting: ParseTree
    expected_with_sorting: ParseTree


TEST_CASES_OPTIMIZER: dict[str, CaseOpt] = {
    "test_only_one_kw": {
        "input_parse_tree": Tree(
            Token("RULE", "query"),
            [Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")])],
        ),
        "expected_kw_merge_without_sorting": Tree(
            Token("RULE", "query"),
            [Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")])],
        ),
        "expected_kw_merge_with_sorting": Tree(
            Token("RULE", "query"),
            [Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")])],
        ),
        "expected_with_sorting": Tree(
            Token("RULE", "query"),
            [Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")])],
        ),
    },
    "test_multiple_kws_with_and": {
        "input_parse_tree": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "conjunction"),
                    [
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                    ],
                )
            ],
        ),
        "expected_kw_merge_without_sorting": Tree(
            Token("RULE", "query"),
            [Tree(Token("RULE", "keyword_op"), [Token("STRING", "'(germany AND france)'")])],
        ),
        "expected_kw_merge_with_sorting": Tree(
            Token("RULE", "query"),
            [Tree(Token("RULE", "keyword_op"), [Token("STRING", "'(germany AND france)'")])],
        ),
        "expected_with_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "conjunction"),
                    [
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                    ],
                )
            ],
        ),
    },
    "test_multiple_kws_with_or": {
        "input_parse_tree": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "disjunction"),
                    [
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                    ],
                )
            ],
        ),
        "expected_kw_merge_without_sorting": Tree(
            Token("RULE", "query"),
            [Tree(Token("RULE", "keyword_op"), [Token("STRING", "'(germany OR france)'")])],
        ),
        "expected_kw_merge_with_sorting": Tree(
            Token("RULE", "query"),
            [Tree(Token("RULE", "keyword_op"), [Token("STRING", "'(germany OR france)'")])],
        ),
        "expected_with_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "disjunction"),
                    [
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                    ],
                )
            ],
        ),
    },
    "test_multiple_kws_with_not_and": {
        "input_parse_tree": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "conjunction"),
                    [
                        Tree(
                            Token("RULE", "negation"),
                            [
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                            ],
                        ),
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                    ],
                )
            ],
        ),
        "expected_kw_merge_without_sorting": Tree(
            Token("RULE", "query"),
            [Tree(Token("RULE", "keyword_op"), [Token("STRING", "'(-: (germany) AND france)'")])],
        ),
        "expected_kw_merge_with_sorting": Tree(
            Token("RULE", "query"),
            [Tree(Token("RULE", "keyword_op"), [Token("STRING", "'(-: (germany) AND france)'")])],
        ),
        "expected_with_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "conjunction"),
                    [
                        Tree(
                            Token("RULE", "negation"),
                            [
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                            ],
                        ),
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                    ],
                )
            ],
        ),
    },
    "test_multiple_kws_with_and_or": {
        "input_parse_tree": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "disjunction"),
                    [
                        Tree(
                            Token("RULE", "conjunction"),
                            [
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                            ],
                        ),
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'spain'")]),
                    ],
                )
            ],
        ),
        "expected_kw_merge_without_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "keyword_op"),
                    [Token("STRING", "'((germany AND france) OR spain)'")],
                )
            ],
        ),
        "expected_kw_merge_with_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "keyword_op"),
                    [Token("STRING", "'(spain OR (germany AND france))'")],
                )
            ],
        ),
        "expected_with_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "disjunction"),
                    [
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'spain'")]),
                        Tree(
                            Token("RULE", "conjunction"),
                            [
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                            ],
                        ),
                    ],
                )
            ],
        ),
    },
    "test_multiple_kws_with_col_in_between_and": {
        "input_parse_tree": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    "conjunction",
                    [
                        Tree(
                            Token("RULE", "conjunction"),
                            [
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                                Tree(
                                    Token("RULE", "col_op"),
                                    [
                                        Tree(
                                            Token("RULE", "percentile_op"),
                                            [
                                                Token("FLOAT", "0.5"),
                                                Token("COMPARISON", "ge"),
                                                Token("SIGNED_NUMBER", "50"),
                                            ],
                                        )
                                    ],
                                ),
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                            ],
                        )
                    ],
                )
            ],
        ),
        "expected_kw_merge_without_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "conjunction"),
                    [
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'(germany)'")]),
                        Tree(
                            Token("RULE", "col_op"),
                            [
                                Tree(
                                    Token("RULE", "percentile_op"),
                                    [
                                        Token("FLOAT", "0.5"),
                                        Token("COMPARISON", "ge"),
                                        Token("SIGNED_NUMBER", "50"),
                                    ],
                                )
                            ],
                        ),
                        Tree(Token("RULE", "keyword_op"), [Token("STRING", "'(france)'")]),
                    ],
                )
            ],
        ),
        "expected_kw_merge_with_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "conjunction"),
                    [
                        Tree(
                            Token("RULE", "keyword_op"),
                            [Token("STRING", "'(germany AND france)'")],
                        ),
                        Tree(
                            Token("RULE", "col_op"),
                            [
                                Tree(
                                    Token("RULE", "percentile_op"),
                                    [
                                        Token("FLOAT", "0.5"),
                                        Token("COMPARISON", "ge"),
                                        Token("SIGNED_NUMBER", "50"),
                                    ],
                                )
                            ],
                        ),
                    ],
                )
            ],
        ),
        "expected_with_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    "conjunction",
                    [
                        Tree(
                            Token("RULE", "conjunction"),
                            [
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                                Tree(
                                    Token("RULE", "col_op"),
                                    [
                                        Tree(
                                            Token("RULE", "percentile_op"),
                                            [
                                                Token("FLOAT", "0.5"),
                                                Token("COMPARISON", "ge"),
                                                Token("SIGNED_NUMBER", "50"),
                                            ],
                                        )
                                    ],
                                ),
                            ],
                        )
                    ],
                )
            ],
        ),
    },
    "test_multiple_kws_with_col_at_the_end_and": {
        "input_parse_tree": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    "conjunction",
                    [
                        Tree(
                            Token("RULE", "conjunction"),
                            [
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                                Tree(
                                    Token("RULE", "col_op"),
                                    [
                                        Tree(
                                            Token("RULE", "percentile_op"),
                                            [
                                                Token("FLOAT", "0.5"),
                                                Token("COMPARISON", "ge"),
                                                Token("SIGNED_NUMBER", "50"),
                                            ],
                                        )
                                    ],
                                ),
                            ],
                        )
                    ],
                )
            ],
        ),
        "expected_kw_merge_without_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "conjunction"),
                    [
                        Tree(
                            Token("RULE", "keyword_op"),
                            [Token("STRING", "'(germany AND france)'")],
                        ),
                        Tree(
                            Token("RULE", "col_op"),
                            [
                                Tree(
                                    Token("RULE", "percentile_op"),
                                    [
                                        Token("FLOAT", "0.5"),
                                        Token("COMPARISON", "ge"),
                                        Token("SIGNED_NUMBER", "50"),
                                    ],
                                )
                            ],
                        ),
                    ],
                )
            ],
        ),
        "expected_kw_merge_with_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    Token("RULE", "conjunction"),
                    [
                        Tree(
                            Token("RULE", "keyword_op"),
                            [Token("STRING", "'(germany AND france)'")],
                        ),
                        Tree(
                            Token("RULE", "col_op"),
                            [
                                Tree(
                                    Token("RULE", "percentile_op"),
                                    [
                                        Token("FLOAT", "0.5"),
                                        Token("COMPARISON", "ge"),
                                        Token("SIGNED_NUMBER", "50"),
                                    ],
                                )
                            ],
                        ),
                    ],
                )
            ],
        ),
        "expected_with_sorting": Tree(
            Token("RULE", "query"),
            [
                Tree(
                    "conjunction",
                    [
                        Tree(
                            Token("RULE", "conjunction"),
                            [
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'germany'")]),
                                Tree(Token("RULE", "keyword_op"), [Token("STRING", "'france'")]),
                                Tree(
                                    Token("RULE", "col_op"),
                                    [
                                        Tree(
                                            Token("RULE", "percentile_op"),
                                            [
                                                Token("FLOAT", "0.5"),
                                                Token("COMPARISON", "ge"),
                                                Token("SIGNED_NUMBER", "50"),
                                            ],
                                        )
                                    ],
                                ),
                            ],
                        )
                    ],
                )
            ],
        ),
    },
}
