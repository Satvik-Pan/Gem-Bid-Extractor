from __future__ import annotations

from pathlib import Path
import re

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

from .settings import COLUMNS


class ExcelWriter:
    _HFILL = PatternFill(start_color="1F4E79", end_color="1F4E79", fill_type="solid")
    _HFONT = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
    _CFONT = Font(name="Calibri", size=10)
    _BORDER = Border(left=Side("thin"), right=Side("thin"), top=Side("thin"), bottom=Side("thin"))

    def __init__(self, path: Path):
        self.path = path

    @staticmethod
    def _looks_like_ref(value: str) -> bool:
        return bool(re.search(r"GEM/\d{4}/[A-Z]/\d+", value or ""))

    @staticmethod
    def _looks_like_date(value: str) -> bool:
        return bool(re.search(r"\d{2}-\d{2}-\d{4}", value or ""))

    @staticmethod
    def _create_empty_workbook(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        wb = Workbook()
        ws = wb.active
        ws.title = "GEM Bids"
        ws.append(COLUMNS)
        for cell in ws[1]:
            cell.font = ExcelWriter._HFONT
            cell.fill = ExcelWriter._HFILL
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.border = ExcelWriter._BORDER
        wb.save(path)
        wb.close()

    @staticmethod
    def _current_header(ws) -> list[str]:
        return [str(c.value or "").strip() for c in ws[1][: len(COLUMNS)]]

    def _migrate_legacy_rows(self, ws) -> list[dict]:
        migrated: list[dict] = []
        for row in ws.iter_rows(min_row=2, values_only=True):
            vals = ["" if v is None else str(v).strip() for v in row]
            vals += [""] * (16 - len(vals))

            ref = vals[1] if self._looks_like_ref(vals[1]) else ""
            if not ref:
                for v in vals:
                    if self._looks_like_ref(v):
                        ref = v
                        break
            if not ref:
                continue

            date_val = vals[2] if self._looks_like_date(vals[2]) else ""
            if not date_val:
                for v in vals:
                    if self._looks_like_date(v):
                        date_val = v
                        break

            name = vals[0] or vals[3]
            description = vals[8] or vals[3] or name
            department = vals[11] or vals[4]
            confidence = vals[13] or ""

            migrated.append(
                {
                    "Category": "",
                    "Reference No.": ref,
                    "Date": date_val,
                    "Name": name,
                    "Start Date": "",
                    "Model - Yr": "",
                    "Quantity": "",
                    "Unit Amount": "",
                    "Description": description,
                    "Contact": "",
                    "EMAIL": "",
                    "Department": department,
                    "Pipeline Source": "legacy_migrated",
                    "LLM Confidence": confidence,
                    "LLM Reason": "Migrated from legacy workbook layout",
                }
            )
        return migrated

    def _ensure_layout(self) -> None:
        if not self.path.exists():
            self._create_empty_workbook(self.path)
            return

        wb = load_workbook(self.path)
        ws = wb.active
        if self._current_header(ws) == COLUMNS:
            wb.close()
            return

        legacy_rows = self._migrate_legacy_rows(ws)
        wb.close()

        self._create_empty_workbook(self.path)
        if not legacy_rows:
            return

        wb2 = load_workbook(self.path)
        ws2 = wb2.active
        seen: set[str] = set()
        for bid in legacy_rows:
            ref = bid.get("Reference No.", "")
            if not ref or ref in seen:
                continue
            seen.add(ref)
            ws2.append([bid.get(col, "") for col in COLUMNS])
            for c in ws2[ws2.max_row]:
                c.font = self._CFONT
                c.border = self._BORDER
                c.alignment = Alignment(vertical="center", wrap_text=True)
        ws2.auto_filter.ref = ws2.dimensions
        wb2.save(self.path)
        wb2.close()

    def _existing_refs(self) -> set[str]:
        self._ensure_layout()
        if not self.path.exists():
            return set()
        wb = load_workbook(self.path)
        ws = wb.active
        col = next((i for i, c in enumerate(ws[1], 1) if c.value == "Reference No."), None)
        if not col:
            wb.close()
            return set()
        refs = {
            str(r[0].value).strip()
            for r in ws.iter_rows(min_row=2, min_col=col, max_col=col)
            if r[0].value
        }
        wb.close()
        return refs

    def save(self, bids: list[dict]) -> int:
        self._ensure_layout()
        refs = self._existing_refs()
        new_bids = [b for b in bids if b.get("Reference No.") and b["Reference No."] not in refs]
        if not new_bids:
            if not self.path.exists():
                self._create_empty_workbook(self.path)
            return 0

        if self.path.exists():
            wb = load_workbook(self.path)
            ws = wb.active
        else:
            self._create_empty_workbook(self.path)
            wb = load_workbook(self.path)
            ws = wb.active

        for bid in new_bids:
            ws.append([bid.get(col, "") for col in COLUMNS])
            for c in ws[ws.max_row]:
                c.font = self._CFONT
                c.border = self._BORDER
                c.alignment = Alignment(vertical="center", wrap_text=True)

        for col in ws.columns:
            max_len = max((len(str(c.value or "")) for c in col), default=0)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 50)

        ws.auto_filter.ref = ws.dimensions
        wb.save(self.path)
        wb.close()
        return len(new_bids)
