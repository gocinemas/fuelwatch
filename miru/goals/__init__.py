"""
Family Goals module for Miru (Phase 2).
Tracks household spending targets and progress.

Exports:
- handle_set_family_goal(from_number, body)
- handle_list_goals(from_number)
- handle_add_household_member(from_number, body)
- link_receipt_to_goals(from_number, amount_pence, category)
"""

from miru.goals.handlers import (
    handle_set_family_goal,
    handle_list_goals,
    handle_add_household_member
)
from miru.goals.spend_linker import link_receipt_to_goals

__all__ = [
    'handle_set_family_goal',
    'handle_list_goals',
    'handle_add_household_member',
    'link_receipt_to_goals'
]
