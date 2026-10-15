from tradingapi.utils import symbol_matches_order

PUT = "SENSEX_OPT_20261015_PUT_71800"
CALL_741 = "SENSEX_OPT_20261015_CALL_74100"
CALL_743 = "SENSEX_OPT_20261015_CALL_74300"
COMBO_741 = f"{PUT}?-20:{CALL_741}?-20"
COMBO_743 = f"{PUT}?-20:{CALL_743}?-20"


def test_shared_put_does_not_match_other_strangle():
    assert symbol_matches_order(COMBO_741, COMBO_743) is False
    assert symbol_matches_order(COMBO_743, COMBO_741) is False


def test_same_combo_matches():
    assert symbol_matches_order(COMBO_741, COMBO_741) is True


def test_single_leg_matches_parent_combo():
    assert symbol_matches_order(PUT, COMBO_743) is True
    assert symbol_matches_order(CALL_741, COMBO_743) is False
