from __future__ import annotations

from typing import Dict, List

from pipelines.steps.validate_data import DataValidator
from pipelines.runner import RunContext


class ValidateCompanies:
    def __init__(self) -> None:
        self.validator = DataValidator()

    def run(self, ctx: RunContext) -> RunContext:
        companies: List[Dict] = ctx.companies or []
        if not companies:
            ctx.companies = []
            return ctx

        # Validate, dedupe, clean (reuse DataValidator helpers)
        valid = self.validator.validate_all_companies(companies)
        unique = self.validator.remove_company_duplicates(valid)
        cleaned = [self.validator.clean_company_data(c) for c in unique]

        ctx.companies = cleaned
        return ctx


