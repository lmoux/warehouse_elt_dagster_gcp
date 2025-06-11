import os
import re
from datetime import datetime, date
from enum import Enum
from io import TextIOWrapper
from urllib.parse import unquote, urlparse, urljoin

import pandas as pd
from bs4 import BeautifulSoup
from py_linq import Enumerable

from src.dw01 import utils


# LMD> py_linq is less pythonic, but more functional. Used while I get reacquainted with Pythonic
# also hoping to exploit lazy evaluation if that library supports it, to be changed later.


class PjmFileKind(Enum):
    FtrSchedule = 1
    FtrBusModelUpdate = 2


class PjmFileProspect:
    def __init__(self, filename: str, url: str, kind: PjmFileKind):
        self.url = url
        self.nice_filename = filename
        self.kind = kind


class PjmFtrScheduleProspect(PjmFileProspect):
    """Represents a PJM FTR Schedule prospect
    and is composed of a URL as well as a nice file name"""

    def __init__(self, filename: str, url: str):
        PjmFileProspect.__init__(self, filename, url, PjmFileKind.FtrSchedule)

    @staticmethod
    def extract_from_url(
            link, base_url="https://www.pjm.com/markets-and-operations/ftr"
    ):
        """Attempts an extraction of an PJM FTR Schedule from a URL pointing an Excel file"""
        if link is None:
            return None
        href = link.get("href")
        if href is None:
            return None

        if ".xls" not in href or "ftr-market-schedule" not in href:
            return None

        filename = unquote(urlparse(href).path.split("/")[-1])
        url = urljoin(base_url, href)
        return PjmFtrScheduleProspect(filename, url)


class PjmFtrBusModelUpdateProspect(PjmFileProspect):
    def __init__(self, filename: str, url: str):
        PjmFileProspect.__init__(self, filename, url, PjmFileKind.FtrBusModelUpdate)

    # TODO: LMD, 2025-06-10> This can almost certainly be refactored along with the Schedule extractor...
    @staticmethod
    def extract_from_url(
            link, base_url="https://www.pjm.com/markets-and-operations/ftr"
    ):
        """Attempts an extraction of an PJM FTR Bus Model change from a URL pointing an Excel file"""
        if link is None:
            return None
        href = link.get("href")
        if href is None:
            return None

        if ".csv" not in href or "ftr-model-update" not in href:
            return None

        filename = unquote(urlparse(href).path.split("/")[-1])
        url = urljoin(base_url, href)
        return PjmFtrBusModelUpdateProspect(filename, url)


class PjmFtrAuction(object):
    """Represents a PJM FTR Schedule"""

    def __init__(
            self,
            version: str,
            market_name: str,
            product: str,
            period: str,
            auction_round: int,
            bidding_opening: datetime,
            bidding_closing: datetime,
            results_posted: datetime,
            contract_start: date,
            contract_end: date,
    ):
        self.version = version
        self.market_name = market_name
        self.product = product
        self.period = period
        self.auction_round = auction_round
        self.bidding_opening = bidding_opening
        self.bidding_closing = bidding_closing
        self.results_posted = results_posted
        self.contract_start = contract_start
        self.contract_end = contract_end

    def __repr__(self):
        return f"({self.market_name}, {self.auction_round}, {self.bidding_opening}, {self.bidding_closing})"


class PjmFtrScheduleFile:
    """Represents a parsed PJM FTR Schedule file"""

    def __init__(self, filename: str):
        self.filename = filename
        self.auctions, self.auction_data_frame = PjmFtrScheduleFile.parse_auctions(filename)

    @staticmethod
    def parse_auctions(filename: str, data=None):
        """Parses a PJM FTR Schedule file"""
        if data is None and filename is None:
            return None
        df = None
        if data is None:
            if filename is None or not os.path.exists(filename):
                return None
            df = pd.read_excel(filename)
        else:
            df = pd.read_csv(data)
        version = int(os.path.basename(filename).split("-")[0])

        # using rename to showcase it, but probably best to use read_excel(name=array) later
        df.rename(
            index={  # because there are some extraneous names in some files
                0: "Market Name",
                1: "Product",
                2: "Period",
                3: "Auction Round",
                4: "Bidding Opening Day",
                5: "Bidding Closing Day",
                6: "Results Posted Day",
                7: "Start Day",
                8: "End Day",
            },
            inplace=True,
        )
        df = df.convert_dtypes()
        df["Start Day"] = pd.to_datetime(df["Start Day"]).dt.date
        df["End Day"] = pd.to_datetime(df["End Day"]).dt.date
        df.dropna(subset=["Start Day", "End Day"], inplace=True)
        # df = df.astype({
        #     'Market Name': str, 'Product': str, 'Period': str, 'Auction Round': str,
        #     'Bidding Opening Day': datetime, 'Bidding Closing Day': datetime, 'Results Posted Day': datetime,
        #     'Start Day': date, 'End Day': date})
        df.insert(0, "version", version)

        # Positional rather than **kwargs because I wanted to use more meaningful names
        return [PjmFtrAuction(*args.values()) for args in df.to_dict(orient="records")], df


class PjmNodeNameChange(object):
    """Represents a PJM (FTR) model Change
    which implies a node name (or ID, or characteristic) change"""

    def __init__(
            self,
            from_id,
            from_txt_zone,
            from_substation,
            from_voltage,
            from_equipment,
            from_name,
            to_id,
            to_txt_zone,
            to_substation,
            to_voltage,
            to_equipment,
            to_name,
    ):
        self.from_id = from_id
        self.from_txt_zone = from_txt_zone
        self.from_substation = from_substation
        self.from_voltage = from_voltage
        self.from_equipment = from_equipment
        self.from_name = from_name
        self.to_id = to_id
        self.to_txt_zone = to_txt_zone
        self.to_substation = to_substation
        self.to_voltage = to_voltage
        self.to_equipment = to_equipment
        self.to_name = to_name


class PjmFtrModelUpdateFile:
    """Represents a PJM FTR model update file"""

    def __init__(self, filename: str):
        self.filename = filename
        with open(filename, "r") as f:
            first_line = f.readline()
            # some have full "FTRs Affected by LMP Bus Model" others just start with FTRs Affected by
            # assert re.match("^FTRs Affected by.*", first_line, re.IGNORECASE) is not None
            assert "FTRs Affected by".upper() in first_line.upper()
            prospect_date = first_line.split("-")[-1].split('"')[0].strip()
            self.version = datetime.strptime(prospect_date, "%B %d, %Y").date()
            f.readline()
            third_line = f.readline()
            assert (
                    re.match(
                        "^((Bus?ses)|(Aggregates)) Changed.*", third_line, re.IGNORECASE
                    )
                    is not None
            )
            # we need to even account for typos...
        df = pd.read_csv(
            filename,
            skiprows=5,
            names=[
                "from_id",
                "from_txt_zone",
                "from_substation",
                "from_voltage",
                "from_equipment",
                "from_name",
                "to_id",
                "to_txt_zone",
                "to_substation",
                "to_voltage",
                "to_equipment",
                "to_name",
            ],
        )
        df.dropna(subset=["to_id"], inplace=True)
        # df.drop(df[~df.to_id.str.isnumeric()].index) #this breaks in some cases
        df.drop(
            df[~df["to_id"].map(lambda x: x.__str__()).str.isnumeric()].index,
            inplace=True,
        )
        df.insert(0, "version", self.version)
        self.changes = df


def url_matches_csv_model_change(link):
    """Returns True when the prospect link is valid, is a CSV within the expected website section"""
    if link is None:
        return False
    href = link.get("href")
    if href is None:
        return False
    return href.endswith(".csv") and "ftr-model-update" in href


def get_urls_all_model_update_files(
        content: str | bytes | TextIOWrapper | None,
        base_url: str = "https://www.pjm.com/markets-and-operations/ftr",
):
    """Retrieves all PJM FTR model nodal name changes links from an HTML
     payload. Assumes the address is constant within PJM.
    The default base_url represents the payload we target by construction."""
    if content is None:
        return []
    soup = BeautifulSoup(content, "html.parser")
    of_interest = Enumerable(soup.find_all("a")).where(url_matches_csv_model_change)

    def extractor(link):
        return PjmFtrBusModelUpdateProspect.extract_from_url(link, base_url)

    return of_interest.select(extractor).where(lambda x: x is not None).to_list()


def get_urls_all_schedule_files(
        content: str | bytes | TextIOWrapper | None,
        base_url: str = "https://www.pjm.com/markets-and-operations/ftr",
) -> list:
    """Retrieves all PJM FTR model nodal name changes links from an HTML
     payload. Assumes the address is constant within PJM.
    The default base_url represents the payload we target by construction."""
    if content is None:
        return []
    soup = BeautifulSoup(content, "html.parser")

    def extractor(x):
        # Apparently, because of E731, this is preferred to assigning a lambda directly.
        # We are essentially doing a partial application (currying in this case, if you will)
        return PjmFtrScheduleProspect.extract_from_url(x, base_url)

    of_interest = Enumerable(soup.find_all("a")).select(extractor)
    return of_interest.where(lambda x: x is not None).to_list()


def get_urls_all_ftr(
        base_url: str = "https://www.pjm.com/markets-and-operations/ftr",
) -> list[PjmFileProspect]:
    base_pjm_ftr_html = utils.download_file_locally(
        base_url, "./downloads/.", overwrite=True
    )
    sought_downloads = []

    # TODO: LMD> This can probably be refactored into a zip like call
    with open(base_pjm_ftr_html, "r") as file:
        for next_download in get_urls_all_model_update_files(file, base_url):
            sought_downloads.append(next_download)
        for next_download in get_urls_all_schedule_files(file, base_url):
            sought_downloads.append(next_download)
    return sought_downloads
