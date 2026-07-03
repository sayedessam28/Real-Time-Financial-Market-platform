"""Unit tests for PriceValidator — no Spark, no Kafka needed."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from producer.services.price_validator import PriceValidator


class TestPriceValidatorBasic:
    def test_first_tick_always_accepted(self):
        v = PriceValidator(threshold_pct=10.0)
        assert v.accept("XAU/USD", 4000.0) is True

    def test_none_always_rejected(self):
        v = PriceValidator(threshold_pct=10.0)
        assert v.accept("XAU/USD", None) is False

    def test_within_threshold_accepted(self):
        v = PriceValidator(threshold_pct=10.0)
        v.accept("XAU/USD", 4000.0)
        assert v.accept("XAU/USD", 4300.0) is True   # 7.5% < 10%

    def test_above_threshold_rejected(self):
        v = PriceValidator(threshold_pct=10.0)
        v.accept("XAU/USD", 4000.0)
        assert v.accept("XAU/USD", 4500.0) is False   # 12.5% > 10%

    def test_rejected_price_does_not_update_last(self):
        v = PriceValidator(threshold_pct=10.0)
        v.accept("XAU/USD", 4000.0)
        v.accept("XAU/USD", 4500.0)   # rejected
        # Next tick should still compare against 4000, not 4500
        assert v.last_price("XAU/USD") == 4000.0

    def test_symbols_tracked_independently(self):
        v = PriceValidator(threshold_pct=10.0)
        v.accept("XAU/USD", 4000.0)
        v.accept("USD/EGP", 50.0)
        # Large move on USD/EGP shouldn't affect XAU/USD baseline
        assert v.accept("XAU/USD", 4300.0) is True
        assert v.accept("USD/EGP", 60.0)   is False   # 20% > 10%


class TestPriceValidatorEdgeCases:
    def test_exactly_at_threshold_accepted(self):
        v = PriceValidator(threshold_pct=10.0)
        v.accept("XAU/USD", 4000.0)
        # Exactly 10% — should be accepted (threshold is strict >)
        assert v.accept("XAU/USD", 4400.0) is True

    def test_drop_also_checked(self):
        v = PriceValidator(threshold_pct=10.0)
        v.accept("XAU/USD", 4000.0)
        assert v.accept("XAU/USD", 3500.0) is False   # -12.5%

    def test_local_gold_tighter_threshold(self):
        """Local gold producer uses threshold_pct=5.0."""
        v = PriceValidator(threshold_pct=5.0)
        v.accept("XAU/EGP_LOCAL", 6000.0)
        assert v.accept("XAU/EGP_LOCAL", 6400.0) is False  # 6.7% > 5%
        assert v.accept("XAU/EGP_LOCAL", 6200.0) is True   # 3.3% < 5%

    def test_observed_outlier_from_logs(self):
        """
        Reproduce the actual outlier seen in production:
        4066.5 → 4192.1 in 2 seconds = 3.09% — below 10% threshold.

        Wait — this is actually within threshold! The real problem was that
        the API returned 4066 and 4192 as two *different* sequential values,
        both sent to Kafka, causing two gap calculations with different
        expected prices in the same second.

        The validator correctly accepts both (each is < 10% from the last),
        but the gap_detector's local_age guard now prevents the second one
        from firing a gap alert if local price is stale.
        """
        v = PriceValidator(threshold_pct=10.0)
        v.accept("XAU/USD", 4066.5)
        # 3.09% change — within threshold, will be accepted (correct)
        assert v.accept("XAU/USD", 4192.1) is True