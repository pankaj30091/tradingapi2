import csv
import datetime as dt
import io
import json
import re
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

import requests

from . import trading_logger
from .exceptions import DataError, MarginError, create_error_context

SPAN_DAILY_REPORTS_URL = "https://www.nseindia.com/api/daily-reports?key=FO"
_NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/octet-stream,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://www.nseindia.com/",
}
_MONTH_MAP = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}
_OPTION_TYPES = {"CE", "PE", "CALL", "PUT", "FUT", "FUTIDX", "FUTSTK", "FUTCOM"}


def _normalize_symbol(symbol: str) -> str:
    return "".join(ch for ch in str(symbol or "").upper() if ch.isalnum())


def _format_strike(value: Optional[float]) -> str:
    if value is None:
        return ""
    return ("%f" % float(value)).rstrip("0").rstrip(".")


def _parse_date(value: Any) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        text = value.strip()
        for fmt in ("%Y%m%d", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%d-%b-%y"):
            try:
                return dt.datetime.strptime(text, fmt).date()
            except ValueError:
                continue
    raise ValueError(f"Unsupported trade date: {value}")


def _looks_like_number(value: str) -> bool:
    try:
        float(str(value).replace(",", ""))
        return True
    except Exception:
        return False


def _to_float(value: str) -> float:
    return float(str(value).replace(",", ""))


def _parse_expiry_token(value: str) -> Optional[dt.date]:
    text = str(value or "").strip().upper()
    if not text:
        return None
    for fmt in ("%Y%m%d", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d%b%Y", "%d%b%y", "%d-%b-%Y", "%d-%b-%y"):
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    if re.fullmatch(r"\d{2}[A-Z]{3}\d{2}", text):
        return dt.date(2000 + int(text[5:7]), _MONTH_MAP[text[2:5]], int(text[:2]))
    return None


def _parse_contract_symbol(raw_symbol: str) -> Dict[str, Any]:
    symbol = _normalize_symbol(raw_symbol)
    match = re.fullmatch(
        r"(?P<underlying>[A-Z0-9]+?)(?P<yy>\d{2})(?P<mon>[A-Z]{3})(?:(?P<strike>\d+(?:\d{2})?)(?P<opt>CE|PE)|(?P<fut>FUT))",
        symbol,
    )
    if not match:
        return {}

    groups = match.groupdict()
    year = 2000 + int(groups["yy"])
    month = _MONTH_MAP.get(groups["mon"])
    if not month:
        return {}

    option_type = "FUT" if groups.get("fut") else groups.get("opt")
    strike = float(groups["strike"]) if groups.get("strike") else None
    return {
        "symbol": groups["underlying"],
        "strike": strike,
        "option_type": option_type,
        "expiry": dt.date(year, month, 1),
        "raw_symbol": symbol,
    }


def _parse_internal_long_symbol(long_symbol: str) -> Dict[str, Any]:
    parts = str(long_symbol or "").upper().split("_")
    if len(parts) < 3:
        return {}
    base, instrument = parts[0], parts[1]
    if instrument == "FUT" and len(parts) >= 3:
        expiry = _parse_expiry_token(parts[2])
        return {"symbol": base, "option_type": "FUT", "expiry": expiry}
    if instrument == "OPT" and len(parts) >= 5:
        expiry = _parse_expiry_token(parts[2])
        option_type = "CE" if parts[3] == "CALL" else ("PE" if parts[3] == "PUT" else parts[3])
        strike = float(parts[4]) if _looks_like_number(parts[4]) else None
        return {"symbol": base, "option_type": option_type, "expiry": expiry, "strike": strike}
    return {}


def _candidate_delimiter(line: str) -> Optional[str]:
    candidates = [",", "|", ";", "\t"]
    counts = {candidate: line.count(candidate) for candidate in candidates}
    delimiter, count = max(counts.items(), key=lambda item: item[1])
    return delimiter if count > 0 else None


def _tokenize_line(line: str) -> List[str]:
    delimiter = _candidate_delimiter(line)
    if delimiter:
        return [token.strip().strip('"') for token in line.split(delimiter)]
    return [token.strip() for token in re.split(r"\s+", line.strip()) if token.strip()]


def _find_16_numeric_values(tokens: Sequence[str]) -> Optional[List[float]]:
    numeric_indexes = [i for i, token in enumerate(tokens) if _looks_like_number(token)]
    if len(numeric_indexes) < 16:
        return None
    for start in range(len(tokens) - 15):
        window = tokens[start : start + 16]
        if all(_looks_like_number(token) for token in window):
            return [_to_float(token) for token in window]
    values = [_to_float(tokens[i]) for i in numeric_indexes[-16:]]
    return values if len(values) == 16 else None


@dataclass(frozen=True)
class Contract:
    contract_id: str
    symbol: str
    expiry: Optional[dt.date]
    strike: Optional[float]
    option_type: str
    lot_size: int
    raw_symbol: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)

    def aliases(self) -> set[str]:
        aliases = {
            _normalize_symbol(self.contract_id),
            _normalize_symbol(self.raw_symbol),
        }
        if self.symbol and self.expiry:
            if self.option_type == "FUT":
                aliases.add(_normalize_symbol(f"{self.symbol}{self.expiry:%y%b}FUT"))
                aliases.add(_normalize_symbol(f"{self.symbol}_FUT_{self.expiry:%Y%m%d}__"))
            elif self.option_type in {"CE", "PE"}:
                strike = _format_strike(self.strike)
                long_type = "CALL" if self.option_type == "CE" else "PUT"
                aliases.add(_normalize_symbol(f"{self.symbol}{self.expiry:%y%b}{strike}{self.option_type}"))
                aliases.add(_normalize_symbol(f"{self.symbol}_OPT_{self.expiry:%Y%m%d}_{long_type}_{strike}"))
        return {alias for alias in aliases if alias}


@dataclass(frozen=True)
class RiskArray:
    contract_id: str
    scenario_values: tuple[float, ...]

    def __post_init__(self) -> None:
        if len(self.scenario_values) != 16:
            raise ValueError("RiskArray requires exactly 16 scenario values")


@dataclass
class Position:
    symbol: str
    quantity: float
    price: float
    contract: Optional[Contract] = None

    def lots(self) -> float:
        if self.contract is None or self.contract.lot_size <= 0:
            return float(self.quantity)
        qty = float(self.quantity)
        lot_size = float(self.contract.lot_size)
        if qty != 0 and abs(qty) >= lot_size and abs(qty / lot_size - round(qty / lot_size)) < 1e-9:
            return qty / lot_size
        return qty

    def signed_units(self) -> float:
        if self.contract is None:
            return float(self.quantity)
        return self.lots() * self.contract.lot_size

    def is_option(self) -> bool:
        return bool(self.contract and self.contract.option_type in {"CE", "PE"})

    def is_future(self) -> bool:
        return bool(self.contract and self.contract.option_type == "FUT")

    def risk_quantity(self, remaining_lots: Optional[float] = None) -> float:
        lots = self.lots() if remaining_lots is None else float(remaining_lots)
        if self.contract and self.contract.metadata.get("source") == "xml-span":
            return lots * self.contract.lot_size
        return lots


@dataclass
class Portfolio:
    positions: List[Position] = field(default_factory=list)

    def add(self, position: Position) -> None:
        self.positions.append(position)


@dataclass
class SpanDataset:
    contracts_by_id: Dict[str, Contract]
    risk_arrays: Dict[str, RiskArray]
    alias_to_contract_id: Dict[str, str]

    def find_contract(self, symbol: str) -> Optional[Contract]:
        normalized = _normalize_symbol(symbol)
        contract_id = self.alias_to_contract_id.get(normalized)
        if contract_id:
            return self.contracts_by_id.get(contract_id)

        parsed = _parse_internal_long_symbol(symbol) or _parse_contract_symbol(symbol)
        if not parsed:
            return None

        matches = []
        for contract in self.contracts_by_id.values():
            if contract.symbol != parsed.get("symbol"):
                continue
            if contract.option_type != parsed.get("option_type"):
                continue
            if parsed.get("strike") is not None and contract.strike != parsed.get("strike"):
                continue
            if parsed.get("expiry") and contract.expiry:
                requested = parsed["expiry"]
                candidate = contract.expiry
                if requested.day == 1:
                    if (requested.year, requested.month) != (candidate.year, candidate.month):
                        continue
                elif requested != candidate:
                    continue
            matches.append(contract)

        if not matches:
            return None
        matches.sort(key=lambda contract: (contract.expiry or dt.date.max, contract.strike or 0.0))
        return matches[0]


class SpanDownloader:
    def __init__(self, download_dir: Path, session: Optional[requests.Session] = None, timeout: int = 30):
        self.download_dir = Path(download_dir)
        self.session = session or requests.Session()
        self.timeout = timeout
        self.session.headers.update(_NSE_HEADERS)

    def download(self, trade_date: dt.date) -> tuple[Path, Path]:
        date_text = trade_date.strftime("%Y%m%d")
        date_dir = self.download_dir / date_text
        date_dir.mkdir(parents=True, exist_ok=True)

        cached = self._find_extracted_files(date_dir)
        if cached:
            return cached

        zip_path = date_dir / f"span_{date_text}.zip"
        try:
            self.session.get("https://www.nseindia.com/", timeout=self.timeout)
            archive_url = self._resolve_archive_url(trade_date)
            response = self.session.get(archive_url, timeout=self.timeout)
            response.raise_for_status()
            if b"<html" in response.content[:512].lower():
                raise DataError(f"NSE SPAN archive returned HTML for {date_text}: {archive_url}")
            zip_path.write_bytes(response.content)
        except Exception as e:
            context = create_error_context(trade_date=date_text, error=str(e))
            raise DataError(f"Failed to download NSE SPAN archive for {date_text}: {str(e)}", context)

        try:
            with zipfile.ZipFile(zip_path) as zip_file:
                zip_file.extractall(date_dir)
        except Exception as e:
            context = create_error_context(trade_date=date_text, zip_path=str(zip_path), error=str(e))
            raise DataError(f"Failed to extract NSE SPAN archive {zip_path.name}: {str(e)}", context)

        extracted = self._find_extracted_files(date_dir)
        if not extracted:
            raise DataError(f"Unable to locate SPN/SPC files in NSE SPAN archive for {date_text}")
        return extracted

    def _resolve_archive_url(self, trade_date: dt.date) -> str:
        report_rows = self._get_live_report_rows()
        if not report_rows:
            return f"https://nsearchives.nseindia.com/archives/nsccl/span/nsccl.{trade_date.strftime('%Y%m%d')}.i1.zip"

        def parse_row_date(row: Dict[str, Any]) -> dt.date:
            return _parse_date(str(row.get("tradingDate", "")).replace("-", "-"))

        def row_position(row: Dict[str, Any]) -> float:
            try:
                return float(row.get("filePosition", 0) or 0)
            except Exception:
                return 0.0

        span_rows = [
            row
            for row in report_rows
            if "SPAN" in str(row.get("displayName", "")).upper()
            or str(row.get("fileKey", "")).upper().startswith("FO-SPAN-")
        ]
        if not span_rows:
            return f"https://nsearchives.nseindia.com/archives/nsccl/span/nsccl.{trade_date.strftime('%Y%m%d')}.i1.zip"

        exact_match_rows = [row for row in span_rows if parse_row_date(row) == trade_date]
        if exact_match_rows:
            selected = max(exact_match_rows, key=row_position)
        else:
            latest_trading_date = max(parse_row_date(row) for row in span_rows)
            latest_rows = [row for row in span_rows if parse_row_date(row) == latest_trading_date]
            selected = max(latest_rows, key=row_position)
        return f"{selected['filePath']}{selected['fileActlName']}"

    def _get_live_report_rows(self) -> List[Dict[str, Any]]:
        try:
            response = self.session.get(SPAN_DAILY_REPORTS_URL, timeout=self.timeout)
            response.raise_for_status()
            data = json.loads(response.text)
            rows: List[Dict[str, Any]] = []
            for section_name in ("FutureDay", "CurrentDay", "PreviousDay"):
                section_rows = data.get(section_name) or []
                if isinstance(section_rows, list):
                    rows.extend(row for row in section_rows if isinstance(row, dict))
            return rows
        except Exception as e:
            trading_logger.log_warning("Failed to fetch live NSE daily-reports metadata", {"error": str(e)})
            return []

    def _find_extracted_files(self, directory: Path) -> Optional[tuple[Path, Path]]:
        spn_path = next(iter(sorted(directory.glob("*.spn"))), None)
        spc_path = next(iter(sorted(directory.glob("*.spc"))), None)
        if spn_path and spc_path:
            return spn_path, spc_path
        if spn_path is not None:
            return spn_path, spn_path
        return None


class SpanParser:
    def parse(self, spn_path: Path, spc_path: Path) -> SpanDataset:
        if self._looks_like_xml_span(spn_path):
            return self._parse_xml_span(spn_path)

        contracts = self._parse_spc(spc_path)
        risk_arrays = self._parse_spn(spn_path)

        alias_to_contract_id: Dict[str, str] = {}
        for contract in contracts.values():
            if contract.contract_id not in risk_arrays:
                continue
            for alias in contract.aliases():
                alias_to_contract_id[alias] = contract.contract_id

        return SpanDataset(contracts_by_id=contracts, risk_arrays=risk_arrays, alias_to_contract_id=alias_to_contract_id)

    def _looks_like_xml_span(self, spn_path: Path) -> bool:
        try:
            with spn_path.open("r", encoding="utf-8", errors="ignore") as handle:
                prefix = handle.read(256).lstrip()
            return prefix.startswith("<?xml") or prefix.startswith("<spanFile")
        except Exception:
            return False

    def _parse_xml_span(self, spn_path: Path) -> SpanDataset:
        try:
            root = ET.parse(spn_path).getroot()
        except Exception as e:
            raise DataError(f"Unable to parse XML SPAN file {spn_path}: {str(e)}")

        contracts: Dict[str, Contract] = {}
        risk_arrays: Dict[str, RiskArray] = {}
        alias_to_contract_id: Dict[str, str] = {}

        for pf in root.iter("futPf"):
            pf_code = str(pf.findtext("pfCode", "")).strip().upper()
            for fut in pf.findall("fut"):
                expiry = _parse_expiry_token(fut.findtext("pe", "") or "")
                if expiry is None:
                    continue
                contract_id = f"FUT|{pf_code}|{expiry:%Y%m%d}|{fut.findtext('cId', '')}"
                price = _to_float(fut.findtext("p", "0") or "0")
                contract = Contract(
                    contract_id=contract_id,
                    symbol=_normalize_symbol(pf_code),
                    expiry=expiry,
                    strike=None,
                    option_type="FUT",
                    lot_size=1,
                    raw_symbol=f"{pf_code}{expiry:%y%b}FUT",
                    metadata={"price": price, "source": "xml-span"},
                )
                scenario_values = self._xml_ra_values(fut.find("ra"))
                if scenario_values is None:
                    continue
                contracts[contract_id] = contract
                risk_arrays[contract_id] = RiskArray(contract_id=contract_id, scenario_values=tuple(scenario_values))
                for alias in contract.aliases():
                    alias_to_contract_id[alias] = contract_id

        for pf in root.iter("oopPf"):
            pf_code = str(pf.findtext("pfCode", "")).strip().upper()
            for series in pf.findall("series"):
                expiry = _parse_expiry_token(series.findtext("pe", "") or "")
                if expiry is None:
                    continue
                for opt in series.findall("opt"):
                    option_code = str(opt.findtext("o", "")).strip().upper()
                    option_type = {"C": "CE", "P": "PE"}.get(option_code, option_code)
                    strike_text = opt.findtext("k", "")
                    if option_type not in {"CE", "PE"} or not _looks_like_number(strike_text or ""):
                        continue
                    strike = _to_float(strike_text)
                    contract_id = f"OPT|{pf_code}|{expiry:%Y%m%d}|{option_type}|{_format_strike(strike)}|{opt.findtext('cId', '')}"
                    price = _to_float(opt.findtext("p", "0") or "0")
                    raw_symbol = f"{pf_code}{expiry:%y%b}{_format_strike(strike)}{option_type}"
                    contract = Contract(
                        contract_id=contract_id,
                        symbol=_normalize_symbol(pf_code),
                        expiry=expiry,
                        strike=strike,
                        option_type=option_type,
                        lot_size=1,
                        raw_symbol=raw_symbol,
                        metadata={"price": price, "source": "xml-span"},
                    )
                    scenario_values = self._xml_ra_values(opt.find("ra"))
                    if scenario_values is None:
                        continue
                    contracts[contract_id] = contract
                    risk_arrays[contract_id] = RiskArray(contract_id=contract_id, scenario_values=tuple(scenario_values))
                    for alias in contract.aliases():
                        alias_to_contract_id[alias] = contract_id

        if not contracts or not risk_arrays:
            raise DataError(f"No contracts/risk arrays parsed from XML SPAN file {spn_path}")
        return SpanDataset(contracts_by_id=contracts, risk_arrays=risk_arrays, alias_to_contract_id=alias_to_contract_id)

    def _xml_ra_values(self, ra_element: Optional[ET.Element]) -> Optional[List[float]]:
        if ra_element is None:
            return None
        values = []
        for child in ra_element.findall("a"):
            text = (child.text or "").strip()
            if _looks_like_number(text):
                values.append(_to_float(text))
        return values[:16] if len(values) >= 16 else None

    def _parse_spn(self, spn_path: Path) -> Dict[str, RiskArray]:
        risk_arrays: Dict[str, RiskArray] = {}
        for raw_line in spn_path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = raw_line.strip()
            if not line or not line.startswith("RA"):
                continue

            tokens = _tokenize_line(line)
            if len(tokens) < 3:
                continue
            if tokens[0] != "RA" and tokens[0] != "RA1":
                tokens = ["RA"] + tokens[0][2:].split()
            contract_id = str(tokens[1]).strip()
            scenario_values = _find_16_numeric_values(tokens[2:])
            if not contract_id or scenario_values is None:
                trading_logger.log_warning("Skipping malformed SPN risk array line", {"line": line[:200]})
                continue

            risk_arrays[contract_id] = RiskArray(contract_id=contract_id, scenario_values=tuple(scenario_values))

        if not risk_arrays:
            raise DataError(f"No risk array records parsed from {spn_path}")
        return risk_arrays

    def _parse_spc(self, spc_path: Path) -> Dict[str, Contract]:
        lines = [line.strip() for line in spc_path.read_text(encoding="utf-8", errors="ignore").splitlines() if line.strip()]
        if not lines:
            raise DataError(f"No contract records found in {spc_path}")

        delimiter = _candidate_delimiter(lines[0]) or ","
        lowered = [token.strip().lower() for token in lines[0].split(delimiter)]
        if any("contract" in token or "symbol" in token for token in lowered):
            return self._parse_spc_with_header(lines, delimiter)
        return self._parse_spc_without_header(lines)

    def _parse_spc_with_header(self, lines: List[str], delimiter: str) -> Dict[str, Contract]:
        reader = csv.DictReader(io.StringIO("\n".join(lines)), delimiter=delimiter)
        contracts: Dict[str, Contract] = {}
        for row in reader:
            contract = self._contract_from_row(row)
            if contract:
                contracts[contract.contract_id] = contract
        if not contracts:
            raise DataError("Unable to parse any contracts from SPC file with header")
        return contracts

    def _parse_spc_without_header(self, lines: List[str]) -> Dict[str, Contract]:
        contracts: Dict[str, Contract] = {}
        for line in lines:
            tokens = _tokenize_line(line)
            if len(tokens) < 3:
                continue
            contract = self._contract_from_tokens(tokens)
            if contract:
                contracts[contract.contract_id] = contract
        if not contracts:
            raise DataError("Unable to parse any contracts from SPC file")
        return contracts

    def _contract_from_row(self, row: Dict[str, Any]) -> Optional[Contract]:
        normalized = {re.sub(r"[^a-z0-9]", "", key.lower()): value for key, value in row.items() if key}
        contract_id = str(
            normalized.get("contractid")
            or normalized.get("instrumentid")
            or normalized.get("token")
            or normalized.get("id")
            or ""
        ).strip()
        raw_symbol = str(
            normalized.get("symbol")
            or normalized.get("tradingsymbol")
            or normalized.get("contract")
            or normalized.get("contractsymbol")
            or ""
        ).strip()
        parsed_symbol = _parse_contract_symbol(raw_symbol)

        option_type = str(normalized.get("optiontype") or normalized.get("instrumenttype") or parsed_symbol.get("option_type") or "").upper()
        if option_type == "CALL":
            option_type = "CE"
        if option_type == "PUT":
            option_type = "PE"
        if option_type in {"FUTIDX", "FUTSTK", "FUTCOM"}:
            option_type = "FUT"

        expiry = _parse_expiry_token(str(normalized.get("expiry") or normalized.get("expirydate") or "")) or parsed_symbol.get("expiry")
        strike = None
        if normalized.get("strike") and _looks_like_number(str(normalized["strike"])):
            strike = _to_float(str(normalized["strike"]))
        elif parsed_symbol.get("strike") is not None:
            strike = float(parsed_symbol["strike"])

        symbol = str(normalized.get("underlying") or parsed_symbol.get("symbol") or raw_symbol).strip().upper()
        lot_value = normalized.get("lotsize") or normalized.get("marketlot") or normalized.get("qty") or 0
        if not contract_id or not raw_symbol or not lot_value or not option_type:
            return None
        if not _looks_like_number(str(lot_value)):
            return None

        return Contract(
            contract_id=contract_id,
            symbol=_normalize_symbol(symbol),
            expiry=expiry,
            strike=strike,
            option_type=option_type,
            lot_size=int(round(_to_float(str(lot_value)))),
            raw_symbol=raw_symbol,
            metadata=row,
        )

    def _contract_from_tokens(self, tokens: Sequence[str]) -> Optional[Contract]:
        contract_id = str(tokens[0]).strip()
        raw_symbol = next((token for token in tokens[1:] if token and re.search(r"(CE|PE|FUT)", token.upper())), tokens[1])
        parsed_symbol = _parse_contract_symbol(raw_symbol)
        option_type = str(parsed_symbol.get("option_type") or "").upper()
        symbol = _normalize_symbol(parsed_symbol.get("symbol") or raw_symbol)
        expiry = parsed_symbol.get("expiry")
        strike = parsed_symbol.get("strike")

        explicit_expiry = next((token for token in tokens if _parse_expiry_token(token)), None)
        if explicit_expiry:
            expiry = _parse_expiry_token(explicit_expiry)

        lot_size_token = next((token for token in reversed(tokens) if token.isdigit() and int(token) > 0), None)
        if not contract_id or not raw_symbol or not option_type or not lot_size_token:
            return None

        return Contract(
            contract_id=contract_id,
            symbol=symbol,
            expiry=expiry,
            strike=float(strike) if strike is not None else None,
            option_type=option_type,
            lot_size=int(lot_size_token),
            raw_symbol=raw_symbol,
            metadata={"tokens": list(tokens)},
        )


@dataclass(frozen=True)
class VerticalSpread:
    short_symbol: str
    long_symbol: str
    hedged_lots: float
    spread_margin: float


class SpanEngine:
    def __init__(
        self,
        trade_date: Optional[dt.date | str] = None,
        download_dir: Optional[str | Path] = None,
        downloader: Optional[SpanDownloader] = None,
        parser: Optional[SpanParser] = None,
        lot_size_lookup: Optional[Callable[[str], int]] = None,
    ):
        self.trade_date = _parse_date(trade_date or dt.date.today())
        self.download_dir = Path(download_dir) if download_dir else Path.cwd() / "span_cache"
        self.downloader = downloader or SpanDownloader(self.download_dir)
        self.parser = parser or SpanParser()
        self.lot_size_lookup = lot_size_lookup
        self.dataset: Optional[SpanDataset] = None
        self.portfolio = Portfolio()

    def load_span(
        self,
        trade_date: Optional[dt.date | str] = None,
        spn_path: Optional[str | Path] = None,
        spc_path: Optional[str | Path] = None,
    ) -> "SpanEngine":
        if trade_date is not None:
            self.trade_date = _parse_date(trade_date)
        if spn_path and spc_path:
            self.dataset = self.parser.parse(Path(spn_path), Path(spc_path))
            return self

        spn_file, spc_file = self.downloader.download(self.trade_date)
        self.dataset = self.parser.parse(spn_file, spc_file)
        return self

    def add_position(self, symbol: str, qty: float, price: float) -> None:
        self.portfolio.add(Position(symbol=symbol, quantity=float(qty), price=float(price)))

    def calculate_margin(self) -> Dict[str, Any]:
        if self.dataset is None:
            self.load_span(self.trade_date)

        assert self.dataset is not None
        resolved_positions: List[Position] = []
        unresolved_symbols: List[str] = []
        for position in self.portfolio.positions:
            contract = self.dataset.find_contract(position.symbol)
            if contract is None:
                trading_logger.log_warning("SPAN contract not found", {"symbol": position.symbol, "trade_date": str(self.trade_date)})
                unresolved_symbols.append(f"{position.symbol} (no contract match)")
                continue
            if contract.lot_size <= 1 and self.lot_size_lookup is not None:
                try:
                    resolved_lot_size = int(self.lot_size_lookup(position.symbol))
                    if resolved_lot_size > 0:
                        contract = replace(contract, lot_size=resolved_lot_size)
                except Exception:
                    pass
            if contract.contract_id not in self.dataset.risk_arrays:
                trading_logger.log_warning("SPAN risk array not found", {"symbol": position.symbol, "contract_id": contract.contract_id})
                unresolved_symbols.append(f"{position.symbol} (no risk array for contract_id={contract.contract_id})")
                continue
            resolved_positions.append(Position(symbol=position.symbol, quantity=position.quantity, price=position.price, contract=contract))

        if not resolved_positions:
            detail = "; ".join(unresolved_symbols) if unresolved_symbols else "no positions added"
            raise MarginError(f"No positions could be resolved against the loaded SPAN dataset ({self.trade_date}): {detail}")

        spread_pairs = self._detect_vertical_spreads(resolved_positions)
        spread_margin_total = sum(pair.spread_margin for pair in spread_pairs)
        remaining_lots = self._remaining_position_lots(resolved_positions, spread_pairs)

        scenario_pnl = [spread_margin_total] * 16
        for position in resolved_positions:
            risk_array = self.dataset.risk_arrays[position.contract.contract_id]
            lots = remaining_lots.get(position.symbol, 0.0)
            if abs(lots) < 1e-12:
                continue
            risk_qty = position.risk_quantity(remaining_lots=lots)
            for index, value in enumerate(risk_array.scenario_values):
                scenario_pnl[index] += risk_qty * value

        span_margin = max(max(scenario_pnl), 0.0)
        exposure_margin = self._calculate_exposure_margin(resolved_positions)
        option_min_margin = self._calculate_short_option_minimum(resolved_positions)
        premium_credit = self._calculate_premium_credit(resolved_positions)
        total_margin = (span_margin + exposure_margin + option_min_margin - premium_credit) * 1.02

        return {
            "span_margin": float(span_margin),
            "exposure_margin": float(exposure_margin),
            "option_min_margin": float(option_min_margin),
            "premium_credit": float(premium_credit),
            "total_margin": float(total_margin),
            "scenario_pnl": [float(value) for value in scenario_pnl],
        }

    def _remaining_position_lots(
        self, positions: Sequence[Position], spread_pairs: Sequence[VerticalSpread]
    ) -> Dict[str, float]:
        remaining = {position.symbol: position.lots() for position in positions}
        for pair in spread_pairs:
            remaining[pair.short_symbol] = remaining.get(pair.short_symbol, 0.0) + pair.hedged_lots
            remaining[pair.long_symbol] = remaining.get(pair.long_symbol, 0.0) - pair.hedged_lots
        return remaining

    def _detect_vertical_spreads(self, positions: Sequence[Position]) -> List[VerticalSpread]:
        grouped: Dict[tuple[str, Optional[dt.date], str], List[Position]] = {}
        for position in positions:
            if not position.is_option():
                continue
            assert position.contract is not None
            key = (position.contract.symbol, position.contract.expiry, position.contract.option_type)
            grouped.setdefault(key, []).append(position)

        spreads: List[VerticalSpread] = []
        for (_, _, option_type), group_positions in grouped.items():
            longs = [position for position in group_positions if position.lots() > 0]
            shorts = [position for position in group_positions if position.lots() < 0]
            long_available = {position.symbol: position.lots() for position in longs}

            for short_position in shorts:
                assert short_position.contract is not None
                short_lots = abs(short_position.lots())
                if short_lots <= 0:
                    continue

                if option_type == "CE":
                    candidates = sorted(
                        (
                            position
                            for position in longs
                            if position.contract and position.contract.strike is not None and short_position.contract.strike is not None
                            and position.contract.strike > short_position.contract.strike
                            and long_available.get(position.symbol, 0.0) > 0
                        ),
                        key=lambda position: position.contract.strike or float("inf"),
                    )
                else:
                    candidates = sorted(
                        (
                            position
                            for position in longs
                            if position.contract and position.contract.strike is not None and short_position.contract.strike is not None
                            and position.contract.strike < short_position.contract.strike
                            and long_available.get(position.symbol, 0.0) > 0
                        ),
                        key=lambda position: -(position.contract.strike or 0.0),
                    )

                for long_position in candidates:
                    available = long_available.get(long_position.symbol, 0.0)
                    if available <= 0 or short_lots <= 0:
                        continue
                    hedged_lots = min(short_lots, available)
                    assert long_position.contract is not None
                    strike_diff = abs((short_position.contract.strike or 0.0) - (long_position.contract.strike or 0.0))
                    if strike_diff <= 0:
                        continue
                    spreads.append(
                        VerticalSpread(
                            short_symbol=short_position.symbol,
                            long_symbol=long_position.symbol,
                            hedged_lots=hedged_lots,
                            spread_margin=strike_diff * short_position.contract.lot_size * hedged_lots,
                        )
                    )
                    short_lots -= hedged_lots
                    long_available[long_position.symbol] = available - hedged_lots

        return spreads

    def _calculate_exposure_margin(self, positions: Sequence[Position]) -> float:
        exposure = 0.0
        for position in positions:
            notional = abs(position.lots() * position.price * (position.contract.lot_size if position.contract else 1))
            if position.is_future():
                exposure += notional * 0.03
            elif position.is_option():
                exposure += notional * 0.05
        return exposure

    def _calculate_short_option_minimum(self, positions: Sequence[Position]) -> float:
        minimum_margin = 0.0
        for position in positions:
            if position.is_option() and position.lots() < 0:
                minimum_margin += abs(position.lots()) * 50000.0
        return minimum_margin

    def _calculate_premium_credit(self, positions: Sequence[Position]) -> float:
        premium = 0.0
        for position in positions:
            if position.is_option():
                premium += -(position.lots() * position.price * position.contract.lot_size)
        return max(premium, 0.0)


def main() -> None:
    engine = SpanEngine("20240501")
    engine.add_position("NIFTY24MAYFUT", 50, 22500)
    engine.add_position("NIFTY24MAY22500CE", -50, 150)
    result = engine.calculate_margin()
    print(result)


if __name__ == "__main__":
    main()
