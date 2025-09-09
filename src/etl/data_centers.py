import time
import requests

import duckdb
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# 2025-09-07 154 total
# 1.1 North Central US-Illinois
# 2.1 Chicago LaSalle Data Center
# 3.1 Chicago 2 Data Center
# 4.1 ORD2 Elk Grove Data Center


URL = "https://www.datacenters.com/locations/united-states/illinois"
BTN = "button.Control__control__ijHLR.Pagination__pageItem__NsQSw.Pagination__symbol__KHv6r"


def _extract_data_centers(page_html) -> list[dict[str, str]]:
    """Extract data centers from current page HTML"""
    soup = BeautifulSoup(page_html, "html.parser")
    records = []
    cards = soup.select("div.flex.flex-col.gap-1")

    for card in cards:
        name = card.select_one("div.text.font-medium.hover\\:text-purple")
        address = card.select("div.text-xs.text-gray-500")

        if name and len(address) > 1:
            records.append(
                {
                    "name": name.get_text(strip=True),
                    "address": address[1].get_text(strip=True),
                }
            )

    return records


def _click_accept_all_cookies(driver: webdriver.Chrome) -> None:
    """Handle cookie consent if present"""
    try:
        wait = WebDriverWait(driver, 10)
        btn = wait.until(
            EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler"))
        )
        driver.execute_script("arguments[0].click();", btn)
        time.sleep(2)
        print("Accepted cookies")
    except TimeoutException:
        print("No cookie banner found")


def scrape_dc_pages() -> list[dict[str, str]]:
    chrome_options = Options()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get(URL)
        _click_accept_all_cookies(driver)

        # Wait for initial page load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.flex.flex-col.gap-1"))
        )
        time.sleep(3)

        all_records = []
        page_num = 1
        max_pages = 10  # sanity check--there are only about 4 pages

        while page_num <= max_pages:
            print(f"\n=== Processing page {page_num} ===")

            # Extract data from current page
            records = _extract_data_centers(driver.page_source)
            all_records.extend(records)
            print(
                f"Page {page_num}: Found {len(records)} records (Total: {len(all_records)})"
            )

            if records:
                print(f"  First record on page: {records[0]['name']}")

            if page_num >= max_pages:
                print(f"Reached maximum expected pages ({max_pages})")
                break

            # The next button
            try:
                next_btns = driver.find_elements(By.CSS_SELECTOR, BTN)
                print(f"Found {len(next_btns)} pagination buttons")

                if not next_btns:
                    print("No next button found")
                    break

                # Use the last button (should be the "next" arrow)
                next_btn = next_btns[-1]
                btn_classes = next_btn.get_attribute("class")
                print(f"Next button classes: {btn_classes}")

                # Check if disabled
                if "Pagination__disabled__FbUC6" in btn_classes:
                    print("✓ Next button is disabled - reached last page")
                    break

                print("✓ Next button is enabled - clicking...")

                # Get current page source hash for comparison
                current_page_hash = hash(driver.page_source[:1000])

                # Desperation clicks using multiple methods...
                try:
                    # Method 1: Regular click
                    next_btn.click()
                except:  # noqa: E722
                    try:
                        # Method 2: JavaScript click
                        driver.execute_script("arguments[0].click();", next_btn)
                    except:  # noqa: E722
                        # Method 3: Action chains click
                        from selenium.webdriver.common.action_chains import ActionChains

                        ActionChains(driver).move_to_element(next_btn).click().perform()

                print("Button clicked, waiting for page change...")

                page_changed = False
                for i in range(10):  # Wait up to 10 seconds
                    time.sleep(1)
                    new_page_hash = hash(driver.page_source[:1000])
                    if new_page_hash != current_page_hash:
                        print(f"✓ Page changed after {i + 1} seconds")
                        page_changed = True
                        break
                    print(f"Waiting for page change... {i + 1}s")

                if not page_changed:
                    print("⚠ Page didn't change - trying to continue anyway")

                time.sleep(2)
                page_num += 1

            except Exception as e:
                print(f"Error with pagination: {e}")
                break

        print("\n=== Scraping Complete ===")
        print(f"Total pages processed: {page_num}")
        print(f"Total records collected: {len(all_records)}")

        # Verify we got approximately 154 records (40+40+40+34)
        expected_total = 154
        print(f"Expected ~{expected_total} records, got {len(all_records)}")

        return all_records

    except Exception as e:
        print(f"Error during scraping: {e}")
        import traceback

        traceback.print_exc()
        return []

    finally:
        driver.quit()


##############################################################################


def initialize_data_center_table(db_path: str, records: list[dict[str, str]]):
    data_center_df = pd.DataFrame(records)

    con = duckdb.connect(db_path)

    con.register("data_center_df", data_center_df)

    con.execute("""
    CREATE OR REPLACE TABLE data_centers AS
    SELECT * FROM data_center_df
    """)

    con.close()


##############################################################################


def _geocode_address(address: str) -> tuple[float | None, float | None]:
    """Geocode with Nominatim (free, no API key)"""

    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": address,
        "format": "json",
        "limit": 1,
        "countrycodes": "us",
    }
    headers = {"User-Agent": "DataCenterGeocoder/1.0 (bryan@purr.io)"}

    try:
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
    except Exception as e:
        print(f"Error geocoding {address}: {e}")

    return None, None


def _add_column_if_not_exists(db_path: str, table: str, column: str, dtype: str):
    """because duckdb has no IF NOT EXISTS for ADD COLUMN"""
    con = duckdb.connect(db_path)
    cols = con.execute(f"PRAGMA table_info({table});").df()["name"].tolist()
    if column not in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {dtype};")
        print(f"added column: {column} to {db_path}|{table}")


def update_dc_table_with_geocode(db_path: str):
    con = duckdb.connect(db_path)

    _add_column_if_not_exists(db_path, "data_centers", "latitude", "DOUBLE")
    _add_column_if_not_exists(db_path, "data_centers", "longitude", "DOUBLE")

    df = con.execute("SELECT name, address FROM data_centers").df()

    countdown = len(df)

    for idx, row in df.iterrows():
        lat, lon = _geocode_address(row["address"])
        print(f"[{countdown} remaining]: {row['address']} => {lat}, {lon}")
        con.execute(
            """UPDATE data_centers 
        SET latitude=?, longitude=?
        WHERE name=? AND address=?
        """,
            [lat, lon, row["name"], row["address"]],
        )
        countdown -= 1

    con.close()
