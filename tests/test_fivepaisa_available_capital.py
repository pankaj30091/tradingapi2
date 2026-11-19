import sys

sys.path.insert(0, "/home/psharma/onedrive/code/tradingapi2")

from tradingapi.fivepaisa import _cash_and_collateral_from_margin


def test_total_collateral_when_stock_field_is_zero():
    cash, collateral = _cash_and_collateral_from_margin(
        {
            "Ledgerbalance": 353996.5,
            "FundsPayln": 0,
            "MFCollateralValueAfterHaircut": 8613729.14,
            "CollateralValueAfterHairCut": 0,
            "TotalCollateralValue": 14131237.12,
        }
    )
    assert cash == 353996.5
    assert collateral == 14131237.12
    assert round(cash + collateral, 2) == 14485233.62


def test_split_collateral_when_it_is_larger_than_total():
    cash, collateral = _cash_and_collateral_from_margin(
        {
            "Ledgerbalance": 100.0,
            "FundsPayln": 20.0,
            "MFCollateralValueAfterHaircut": 50.0,
            "CollateralValueAfterHairCut": 80.0,
            "TotalCollateralValue": 100.0,
        }
    )
    assert cash == 120.0
    assert collateral == 130.0
