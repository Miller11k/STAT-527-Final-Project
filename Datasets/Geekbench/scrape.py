import re
import time
import argparse
from typing import Optional, List, Dict

import requests
from bs4 import BeautifulSoup
import pandas as pd

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

MHZ_RE   = re.compile(r"(\d+(?:\.\d+)?)\s*MHz", re.I)
CORES_RE = re.compile(r"\((\d+)\s+cores?\)", re.I)
WS_RE    = re.compile(r"\s+")

def oneline(s: str) -> str:
    return WS_RE.sub(" ", s).strip()

def get_soup(session: requests.Session, url: str) -> BeautifulSoup:
    for attempt in range(3):
        r = session.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return BeautifulSoup(r.text, "html.parser")
        time.sleep(1 + attempt)
    r.raise_for_status()

def detect_max_pages(soup: BeautifulSoup) -> int:
    max_page = 1
    pag = soup.select_one("ul.pagination")
    if not pag:
        return max_page
    nums = []
    for a in pag.find_all("a"):
        t = (a.get_text() or "").strip()
        if t.isdigit():
            nums.append(int(t))
    return max(nums) if nums else max_page

# ---------------- CPU v4 ----------------

def parse_v4_row(tr) -> Optional[Dict]:
    tds = tr.find_all("td")
    if len(tds) != 6:
        return None
    if not tds[0].find("span", class_="timestamp-to-local-min"):
        return None

    uploaded = oneline(tds[0].get_text(" ", strip=True))
    model_a = tds[1].find("a")
    system = oneline(model_a.get_text(" ", strip=True) if model_a else tds[1].get_text(" ", strip=True))

    details_span = tds[1].find("span")
    cpu_details, mhz, cores = "", None, None
    if details_span:
        details_text = oneline(details_span.get_text(" ", strip=True))
        cpu_details = details_text
        if m := MHZ_RE.search(details_text):
            try: mhz = float(m.group(1))
            except ValueError: pass
        if m := CORES_RE.search(details_text):
            cores = int(m.group(1))

    platform = oneline(tds[2].get_text(" ", strip=True))
    user_a = tds[3].find("a")
    user = oneline(user_a.get_text(" ", strip=True)) if user_a else ""

    def to_int(s: str) -> Optional[int]:
        s = s.replace(",", "").strip()
        return int(s) if re.fullmatch(r"\d+", s) else None

    single = to_int(oneline(tds[4].get_text(" ", strip=True)))
    multi  = to_int(oneline(tds[5].get_text(" ", strip=True)))

    return {
        "Uploaded": uploaded,
        "System": system,
        "CPU Details": cpu_details,
        "Frequency_MHz": mhz,
        "Cores": cores,
        "Platform": platform,
        "User": user,
        "Single-Core Score": single,
        "Multi-Core Score": multi,
        "Result URL": model_a["href"] if model_a and model_a.has_attr("href") else "",
        "Schema": "v4",
    }

def parse_v4_page(soup: BeautifulSoup) -> List[Dict]:
    out = []
    table = soup.select_one("table.geekbench3-index")
    if not table:
        return out
    for tr in table.find_all("tr"):
        row = parse_v4_row(tr)
        if row:
            out.append(row)
    return out

# ---------------- CPU v6 ----------------

def _text(el) -> str:
    return oneline(el.get_text(" ", strip=True)) if el else ""

def _to_int(s: str) -> Optional[int]:
    s = s.replace(",", "").strip()
    return int(s) if re.fullmatch(r"\d+", s) else None

def parse_v6_cpu_block(inner: BeautifulSoup) -> Optional[Dict]:
    data = {
        "Uploaded": "", "System": "", "CPU Details": "", "Frequency_MHz": None, "Cores": None,
        "Platform": "", "User": "", "Single-Core Score": None, "Multi-Core Score": None,
        "Result URL": "", "Schema": "v6",
    }

    sys_col = inner.select_one(".col-12.col-lg-4")
    if sys_col:
        a = sys_col.select_one("a[href^='/v6/cpu/']")
        if a:
            data["System"] = _text(a)
            data["Result URL"] = a["href"]
        model = sys_col.select_one(".list-col-model")
        if model:
            model_text = _text(model)
            data["CPU Details"] = model_text
            if m := MHZ_RE.search(model_text):
                try: data["Frequency_MHz"] = float(m.group(1))
                except ValueError: pass
            if m := CORES_RE.search(model_text):
                data["Cores"] = int(m.group(1))

    for col in inner.select(".col-6.col-md-3.col-lg-2"):
        label = col.select_one(".list-col-subtitle, .list-col-subtitle-score")
        if not label:
            continue
        key = _text(label)
        if key in ("Uploaded", "Platform"):
            val = col.select_one(".list-col-text")
            data[key] = _text(val)
            if key == "Uploaded":
                u = col.select_one("a[href^='/user/']")
                if u:
                    data["User"] = _text(u)
        elif key in ("Single-Core Score", "Multi-Core Score"):
            val = col.select_one(".list-col-text-score")
            iv = _to_int(_text(val))
            if key == "Single-Core Score":
                data["Single-Core Score"] = iv
            else:
                data["Multi-Core Score"] = iv

    if not data["System"] and not data["Result URL"]:
        return None
    return data

def parse_v6_cpu_page(soup: BeautifulSoup) -> List[Dict]:
    out = []
    for inner in soup.select(".list-col .list-col-inner"):
        row = parse_v6_cpu_block(inner)
        if row:
            out.append(row)
    return out

# ---------------- GPU v6 /compute ----------------

def parse_v6_compute_block(inner: BeautifulSoup) -> Optional[Dict]:
    """
    Cards look like:
      System (link /v6/compute/...), model line under it
      Uploaded | Platform | API | <API> Score
    """
    data = {
        "Uploaded": "", "System": "", "CPU Details": "", "Frequency_MHz": None, "Cores": None,
        "Platform": "", "API": "", "Score Label": "", "Compute Score": None,
        "Result URL": "", "Schema": "v6-compute",
    }

    # left column
    sys_col = inner.select_one(".col-12.col-lg-4")
    if sys_col:
        a = sys_col.select_one("a[href^='/v6/compute/']")
        if a:
            data["System"] = _text(a)
            data["Result URL"] = a["href"]
        model = sys_col.select_one(".list-col-model")
        if model:
            model_text = _text(model)  # keep exactly as card shows
            data["CPU Details"] = model_text
            if m := MHZ_RE.search(model_text):
                try: data["Frequency_MHz"] = float(m.group(1))
                except ValueError: pass
            if m := CORES_RE.search(model_text):
                data["Cores"] = int(m.group(1))

    # right columns
    for col in inner.select(".col-6.col-md-3.col-lg-2"):
        label_el = col.select_one(".list-col-subtitle, .list-col-subtitle-score")
        if not label_el:
            continue
        label = _text(label_el)

        if label in ("Uploaded", "Platform", "API"):
            val = col.select_one(".list-col-text")
            if label == "API":
                data["API"] = _text(val)
            else:
                data[label] = _text(val)

        # Score column: label text varies ("Metal Score", "Vulkan Score", "OpenCL Score")
        elif label.endswith("Score"):
            data["Score Label"] = label
            val = col.select_one(".list-col-text-score")
            data["Compute Score"] = _to_int(_text(val))

    if not data["System"] and not data["Result URL"]:
        return None
    return data

def parse_v6_compute_page(soup: BeautifulSoup) -> List[Dict]:
    out = []
    for inner in soup.select(".list-col .list-col-inner"):
        row = parse_v6_compute_block(inner)
        if row:
            out.append(row)
    return out

# ---------------- driver ----------------

def is_v6_cpu(url: str) -> bool:
    return "/v6/cpu" in url

def is_v6_compute(url: str) -> bool:
    return "/v6/compute" in url

def is_v4_cpu(url: str) -> bool:
    return "/v4/cpu" in url

def page_url(base: str, page: int) -> str:
    return f"{base}?page={page}"

def scrape(base: str, max_pages: Optional[int], out_csv: str, sleep_s: float = 0.5) -> pd.DataFrame:
    session = requests.Session()
    first = get_soup(session, page_url(base, 1))
    detected = detect_max_pages(first)
    pages = max_pages if (max_pages and max_pages > 0) else detected

    print(f"Scraping {pages} page(s) from {base} "
          f"(max_pages={'provided' if max_pages else 'auto'}: {pages})")

    all_rows: List[Dict] = []

    if is_v6_compute(base):
        mode = "v6-compute"
    elif is_v6_cpu(base):
        mode = "v6-cpu"
    elif is_v4_cpu(base):
        mode = "v4-cpu"
    else:
        raise SystemExit("Unsupported base URL (expect /v6/compute, /v6/cpu, or /v4/cpu).")

    for p in range(1, pages + 1):
        url = page_url(base, p)
        print(f"[{p}/{pages}] GET {url}")
        soup = first if p == 1 else get_soup(session, url)

        if mode == "v6-compute":
            rows = parse_v6_compute_page(soup)
            if not rows:
                print("  ! No v6 compute blocks found on this page.")
        elif mode == "v6-cpu":
            rows = parse_v6_cpu_page(soup)
            if not rows:
                print("  ! No v6 CPU blocks found on this page.")
        else:
            rows = parse_v4_page(soup)
            if not rows:
                print("  ! No v4 CPU rows found on this page.")

        all_rows.extend(rows)
        time.sleep(sleep_s)

    # Column sets
    cpu_cols = [
        "Uploaded","System","CPU Details","Frequency_MHz","Cores","Platform","User",
        "Single-Core Score","Multi-Core Score","Result URL","Schema"
    ]
    gpu_cols = [
        "Uploaded","System","CPU Details","Frequency_MHz","Cores","Platform",
        "API","Score Label","Compute Score","Result URL","Schema"
    ]

    if mode == "v6-compute":
        cols = gpu_cols
    else:
        cols = cpu_cols

    df = pd.DataFrame(all_rows)
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]
    df.to_csv(out_csv, index=False)
    print(f"Saved {len(df):,} rows → {out_csv}")
    return df

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Scrape Geekbench lists (CPU v4/v6, GPU v6 compute) to CSV.")
    ap.add_argument("--base", type=str, required=True,
                    help="List URL, e.g. https://browser.geekbench.com/v6/compute or /v6/cpu/singlecore or /v4/cpu/singlecore")
    ap.add_argument("--max-pages", type=int, default=None,
                    help="Max pages to scrape; if omitted, auto-detects.")
    ap.add_argument("--out", type=str, default="geekbench_results.csv",
                    help="Output CSV path.")
    ap.add_argument("--sleep", type=float, default=0.5,
                    help="Delay between requests (seconds).")
    args = ap.parse_args()
    scrape(args.base.rstrip("/"), args.max_pages, args.out, args.sleep)