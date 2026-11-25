import pytest

from src.model_service import FEATURES, risk_level


@pytest.mark.parametrize(
    "probability, expected",
    [
        (0.0, "Low"),
        (0.29, "Low"),
        (0.30, "Medium"),
        (0.69, "Medium"),
        (0.70, "High"),
        (1.0, "High"),
    ],
)
def test_risk_level_thresholds(probability, expected):
    assert risk_level(probability) == expected


def test_model_takes_the_12_contract_features():
    assert len(FEATURES) == 12
    assert "churned" not in FEATURES
    assert "customer_id" not in FEATURES
