"""Review: "arrives earlier than ours" is queue order (eta, unit_uuid) with our eta at that station."""

import pytest

EAST = 90.0


@pytest.mark.parametrize("other, expected", [("A", ("recommended", "S2", 230)), ("X", ("rejected", None, None))])
def test_equal_eta_is_earlier_only_with_a_smaller_unit_uuid(world, other, expected):
    w = world()
    T0 = w.T0
    w.move(other, T0, 1501.0, EAST)               # eta S1 = T0 + 150
    w.move("T4", T0 - 1, 1506.0)
    d = w.move("T4", T0, 1496.0)[0].value          # eta S1 = T0 + 150 as well
    assert (d["result"], d.get("to_station"), d.get("gain_seconds")) == expected
