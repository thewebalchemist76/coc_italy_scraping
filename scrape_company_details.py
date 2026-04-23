#!/usr/bin/env python3
"""
Scarica i dettagli di tutte le aziende da reportaziende.it:
  https://www.reportaziende.it/JSON/company.php?id=MI_2610233_0

Input: JSON lista aziende (array) con campo "id" (es. aziende_ateco47_0_10M_lombardia.json).

Output (dentro reportaziende_details/):
- Unico file JSONL: <out-base>_details.jsonl (una riga JSON per azienda)
- State per resume: <out-base>_state.json

Resume:
- Se interrompi, rilancia lo stesso comando (stesso --out-base).
- Per ripartire da zero: --fresh
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


API_BASE = "https://www.reportaziende.it/JSON/company.php"
HOME_URL = "https://www.reportaziende.it/"


@dataclass
class State:
    next_index: int = 0
    batch_index: int = 0


def _load_state(state_path: Path) -> State:
    try:
        if not state_path.exists():
            return State()
        with open(state_path, encoding="utf-8") as f:
            raw = json.load(f)
        return State(
            next_index=int(raw.get("next_index", 0)),
            batch_index=int(raw.get("batch_index", 0)),
        )
    except Exception as e:
        print(f"[warn] impossibile leggere state {state_path}: {e}", file=sys.stderr)
        return State()


def _save_state(state_path: Path, st: State) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump({"next_index": st.next_index, "batch_index": st.batch_index}, f)


def _company_url(company_id: str) -> str:
    return f"{API_BASE}?id={company_id}"


def _prime_session(session: requests.Session, cookie: str | None, timeout: int) -> None:
    """
    Alcuni WAF/rate-limit rilasciano cookie su HOME e poi permettono JSON endpoints.
    Se fallisce non blocchiamo lo scraping (best effort).
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
    }
    if cookie:
        headers["Cookie"] = cookie
    try:
        session.get(HOME_URL, headers=headers, timeout=timeout)
    except Exception:
        pass


def _fetch_company(
    company_id: str,
    session: requests.Session,
    cookie: str | None,
    timeout: int,
    retries: int,
    cooldown_on_403: float,
) -> dict[str, Any] | None:
    url = _company_url(company_id)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        "Referer": "https://www.reportaziende.it/",
        "Connection": "keep-alive",
    }
    if cookie:
        headers["Cookie"] = cookie

    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, headers=headers, timeout=timeout)
            if r.status_code == 403:
                # Cooldown + refresh cookie/sessione
                print(f"[warn] 403 su {company_id} (attempt {attempt}/{retries})", file=sys.stderr)
                _prime_session(session, cookie, timeout)
                time.sleep(max(cooldown_on_403, 1.0) * attempt)
                continue
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                return payload["data"]
            return None
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(0.5 * attempt)
            else:
                print(f"[error] {company_id} -> {e}", file=sys.stderr)
    _ = last_err
    return None


def _append_jsonl(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def _append_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Scrape reportaziende.it company.php details in batch con resume")
    ap.add_argument("--input", required=True, type=Path, help="Path JSON lista aziende (array con campo id)")
    ap.add_argument("--out-dir", type=Path, default=Path(__file__).resolve().parent, help="Cartella output (default: reportaziende_details/)")
    ap.add_argument("--out-base", required=True, help="Prefix output (es. ateco47_lombardia_0_10M)")
    ap.add_argument("--sleep", type=float, default=0.2, help="Pausa tra richieste in secondi (default: 0.2)")
    ap.add_argument("--timeout", type=int, default=30, help="Timeout HTTP in secondi (default: 30)")
    ap.add_argument("--retries", type=int, default=3, help="Tentativi per azienda (default: 3)")
    ap.add_argument("--cooldown-on-403", type=float, default=30.0, help="Secondi di pausa quando ricevi 403 (default: 30)")
    ap.add_argument("--cookie", default=None, help="Cookie header (se serve sessione)")
    ap.add_argument("--fresh", action="store_true", help="Ignora state e riparti da zero")
    args = ap.parse_args()

    out_dir: Path = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    state_path = out_dir / f"{args.out_base}_state.json"
    out_jsonl = out_dir / f"{args.out_base}_details.jsonl"
    out_failed = out_dir / f"{args.out_base}_failed_ids.txt"
    st = State() if args.fresh else _load_state(state_path)

    with open(args.input, encoding="utf-8") as f:
        companies_list = json.load(f)
    if not isinstance(companies_list, list):
        raise SystemExit("Input JSON deve essere un array di oggetti con campo 'id'")

    ids: list[str] = []
    for x in companies_list:
        if isinstance(x, dict) and x.get("id"):
            ids.append(str(x["id"]))
    if not ids:
        raise SystemExit("Nessun id trovato nell'input")

    session = requests.Session()
    _prime_session(session, args.cookie, args.timeout)

    total = len(ids)
    start = st.next_index
    if start > 0:
        print(f"[resume] next_index={start} / {total}", file=sys.stderr)

    try:
        for i in range(start, total):
            company_id = ids[i]
            data = _fetch_company(
                company_id,
                session,
                args.cookie,
                args.timeout,
                args.retries,
                args.cooldown_on_403,
            )
            st.next_index = i + 1

            if not data:
                _append_line(out_failed, company_id)
                _save_state(state_path, st)
                continue

            # Unico file: append JSON per azienda
            _append_jsonl(out_jsonl, data)

            if (i + 1) % 50 == 0:
                _save_state(state_path, st)
                print(f"[{i+1}/{total}] ok", file=sys.stderr)

            time.sleep(args.sleep)

        _save_state(state_path, st)
        print(f"[done] scritti dettagli -> {out_jsonl.name} (state {state_path.name})", file=sys.stderr)
    finally:
        pass


if __name__ == "__main__":
    main()

