"""Central registry of all example cases.

Import ALL_CASES for the complete flat list, or import individual module
CASES lists for selective use.
"""
from __future__ import annotations

from examples.cases.c01_basic_select import CASES as C01
from examples.cases.c02_filtering import CASES as C02
from examples.cases.c03_ordering_paging import CASES as C03
from examples.cases.c04_joins import CASES as C04
from examples.cases.c05_aggregations import CASES as C05
from examples.cases.c06_subqueries import CASES as C06
from examples.cases.c07_ctes import CASES as C07
from examples.cases.c08_set_operations import CASES as C08
from examples.cases.c09_window_functions import CASES as C09
from examples.cases.c10_complex import CASES as C10
from examples._case import Case

ALL_CASES: list[Case] = C01 + C02 + C03 + C04 + C05 + C06 + C07 + C08 + C09 + C10

__all__ = [
    "ALL_CASES",
    "C01", "C02", "C03", "C04", "C05",
    "C06", "C07", "C08", "C09", "C10",
]
