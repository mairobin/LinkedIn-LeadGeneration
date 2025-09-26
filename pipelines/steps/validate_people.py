from __future__ import annotations

from typing import List

from pipelines.steps.validate_data import DataValidator
from pipelines.runner import RunContext


class ValidatePeople:
    def __init__(self) -> None:
        self.validator = DataValidator()

    def run(self, ctx: RunContext) -> RunContext:
        people = ctx.people or []
        # Detect if input is already in mapped Person schema (from map_to_person_schema)
        is_mapped = False
        try:
            if people and isinstance(people[0], dict):
                sample = people[0]
                is_mapped = ('LinkedIn_Profile' in sample) or ('Contact_Name' in sample)
        except Exception:
            is_mapped = False

        if is_mapped:
            # For mapped schema, skip strict raw validation; perform light cleaning only
            cleaned = [self.validator.clean_profile_data(p) for p in people]
            valid = cleaned
        else:
            # For raw extracted profiles, run full validation and cleaning
            valid = self.validator.validate_all_profiles(people)
            cleaned = [self.validator.clean_profile_data(p) for p in valid]
        ctx.people = cleaned
        # Attach validation stats into meta for optional logging
        ctx.meta["validation_stats"] = self.validator.get_validation_stats()
        return ctx


