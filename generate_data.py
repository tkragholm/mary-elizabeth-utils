import argparse
import os
from datetime import datetime, timedelta

import numpy as np
import polars as pl
from mary_elizabeth_utils.utils.logger import setup_colored_logger

logger = setup_colored_logger(__name__)


def random_date(start, end):
    return start + timedelta(seconds=np.random.randint(0, int((end - start).total_seconds())))


def generate_pnr_pool(num_pnrs=10000):
    return [f"{np.random.randint(1000000000, 9999999999)}" for _ in range(num_pnrs)]


PNR_POOL = generate_pnr_pool()


def get_random_pnr():
    return np.random.choice(PNR_POOL)


def generate_data(config, num_records, year):
    data = {}
    for col, col_config in config.items():
        if col_config["type"] == "choice":
            data[col] = np.random.choice(col_config["options"], num_records)
        elif col_config["type"] == "int":
            data[col] = np.random.randint(col_config["min"], col_config["max"], num_records)
        elif col_config["type"] == "float":
            data[col] = np.random.normal(col_config["mean"], col_config["std"], num_records)
        elif col_config["type"] == "date":
            start = max(col_config["start"], datetime(year, 1, 1))
            end = min(col_config["end"], datetime(year, 12, 31))
            if start >= end:
                data[col] = [start] * num_records
            else:
                data[col] = [random_date(start, end) for _ in range(num_records)]
        elif col_config["type"] == "pnr":
            data[col] = [get_random_pnr() for _ in range(num_records)]
        elif col_config["type"] == "string":
            data[col] = [
                f"{col_config['prefix']}{np.random.randint(col_config['min'], col_config['max'])}"
                for _ in range(num_records)
            ]
    return pl.DataFrame(data)


# Define configurations for each register
register_configs = {
    "BEF": {
        "PNR": {"type": "pnr"},
        "AEGTE_ID": {"type": "pnr"},
        "ALDER": {"type": "int", "min": 0, "max": 100},
        "ANTBOERNF": {"type": "int", "min": 0, "max": 5},
        "ANTPERSF": {"type": "int", "min": 1, "max": 6},
        "BOP_VFRA": {"type": "date", "start": datetime(1950, 1, 1), "end": datetime(2023, 12, 31)},
        "CIVST": {"type": "choice", "options": ["U", "G", "F", "E", "L"]},
        "CIV_VFRA": {"type": "date", "start": datetime(1950, 1, 1), "end": datetime(2023, 12, 31)},
        "FAMILIE_ID": {"type": "string", "prefix": "F", "min": 100000, "max": 999999},
        "FAMILIE_TYPE": {"type": "choice", "options": ["E", "F", "G"]},
        "FAR_ID": {"type": "pnr"},
        "FOED_DAG": {"type": "date", "start": datetime(1920, 1, 1), "end": datetime(2023, 12, 31)},
        "IE_TYPE": {"type": "choice", "options": ["D", "I", "E"]},
        "KOEN": {"type": "choice", "options": ["M", "K"]},
        "KOM": {"type": "int", "min": 101, "max": 851},
        "MOR_ID": {"type": "pnr"},
        "OPR_LAND": {"type": "choice", "options": ["5100", "5110", "5120", "5130"]},
        "STATSB": {"type": "choice", "options": ["5100", "5110", "5120", "5130"]},
        "VERSION": {"type": "choice", "options": ["1", "2"]},
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
        # Adding the missing variables
        "PLADS": {"type": "choice", "options": ["1", "2", "3", "4", "5"]},  # Familiestatus
        "HUSTYPE": {"type": "choice", "options": ["E", "F", "G", "H", "I"]},  # Husstandstype
    },
    "DOD": {
        "PNR": {"type": "pnr"},
        "DODDATO": {"type": "date", "start": datetime(2000, 1, 1), "end": datetime(2023, 12, 31)},
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
    },
    "DODSAARS": {
        "PNR": {"type": "pnr"},
        "C_DOD1": {"type": "string", "prefix": "A", "min": 10, "max": 99},
        "C_DOD2": {"type": "string", "prefix": "B", "min": 10, "max": 99},
        "C_DOD3": {"type": "string", "prefix": "C", "min": 10, "max": 99},
        "C_DOD4": {"type": "string", "prefix": "D", "min": 10, "max": 99},
        "C_DODSMAADE": {"type": "choice", "options": ["1", "2", "3", "4", "5"]},
        "D_DODSDTO": {"type": "date", "start": datetime(2000, 1, 1), "end": datetime(2023, 12, 31)},
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
    },
    "DODSAASG": {
        "PNR": {"type": "pnr"},
        "C_DODTILGRUNDL_ACME": {"type": "string", "prefix": "E", "min": 10, "max": 99},
        "C_DOD_1A": {"type": "string", "prefix": "F", "min": 10, "max": 99},
        "C_DOD_1B": {"type": "string", "prefix": "G", "min": 10, "max": 99},
        "C_DOD_1C": {"type": "string", "prefix": "H", "min": 10, "max": 99},
        "C_DOD_1D": {"type": "string", "prefix": "I", "min": 10, "max": 99},
        "D_DODSDATO": {
            "type": "date",
            "start": datetime(2000, 1, 1),
            "end": datetime(2023, 12, 31),
        },
        "C_BOPKOMF07": {"type": "int", "min": 101, "max": 851},
    },
    "VNDS": {
        "PNR": {"type": "pnr"},
        "HAEND_DATO": {
            "type": "date",
            "start": datetime(2000, 1, 1),
            "end": datetime(2023, 12, 31),
        },
        "INDUD_KODE": {"type": "choice", "options": ["10", "20", "30", "40", "50"]},
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
    },
    "AKM": {
        "PNR": {"type": "pnr"},
        "SENR": {"type": "int", "min": 10000000, "max": 99999999},
        "SOCIO": {"type": "choice", "options": ["110", "120", "210", "220", "310", "320", "330"]},
        "SOCIO02": {"type": "choice", "options": ["110", "120", "210", "220", "310", "320", "330"]},
        "SOCIO13": {"type": "choice", "options": ["110", "120", "210", "220", "310", "320", "330"]},
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
        "VERSION": {"type": "choice", "options": ["1", "2"]},
    },
    "FAIK": {
        "FAMILIE_ID": {"type": "string", "prefix": "F", "min": 100000, "max": 999999},
        "FAMAEKVIVADISP": {"type": "float", "mean": 300000, "std": 50000},
        "FAMAEKVIVADISP_13": {"type": "float", "mean": 320000, "std": 55000},
        "FAMBOLIGFORM": {"type": "choice", "options": ["1", "2", "3", "4", "5"]},
        "FAMDISPONIBEL": {"type": "float", "mean": 400000, "std": 70000},
        "FAMDISPONIBEL_13": {"type": "float", "mean": 420000, "std": 75000},
        "FAMERHVERVSINDK": {"type": "float", "mean": 450000, "std": 80000},
        "FAMERHVERVSINDK_13": {"type": "float", "mean": 470000, "std": 85000},
        "FAMINDKOMSTIALT": {"type": "float", "mean": 500000, "std": 90000},
        "FAMINDKOMSTIALT_13": {"type": "float", "mean": 520000, "std": 95000},
        "FAMSKATPLIGTINDK": {"type": "float", "mean": 480000, "std": 85000},
        "FAMSOCIOGRUP": {
            "type": "choice",
            "options": ["110", "120", "210", "220", "310", "320", "330"],
        },
        "FAMSOCIOGRUP_13": {
            "type": "choice",
            "options": ["110", "120", "210", "220", "310", "320", "330"],
        },
        "VERSION": {"type": "choice", "options": ["1", "2"]},
    },
    "IDAN": {
        "PNR": {"type": "pnr"},
        "ARBGNR": {"type": "int", "min": 1000000, "max": 9999999},
        "ARBNR": {"type": "int", "min": 1000, "max": 9999},
        "CVRNR": {"type": "int", "min": 10000000, "max": 99999999},
        "JOBKAT": {"type": "choice", "options": ["1", "2", "3", "4", "5"]},
        "JOBLON": {"type": "float", "mean": 400000, "std": 100000},
        "LBNR": {"type": "int", "min": 1, "max": 999},
        "STILL": {"type": "choice", "options": ["1110", "2310", "3320", "4120", "5230"]},
        "TILKNYT": {"type": "choice", "options": ["1", "2", "3"]},
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
    },
    "ILME": {
        "PNR": {"type": "pnr"},
        "VMO_A_INDK_AM_BIDRAG_BETAL": {"type": "float", "mean": 350000, "std": 70000},
        "VMO_A_INDK_AM_BIDRAG_FRI": {"type": "float", "mean": 20000, "std": 5000},
        "VMO_BASIS_AAR": {
            "type": "choice",
            "options": [str(year) for year in range(2009, 2023)],
        },
        "VMO_BASIS_MAANED": {"type": "int", "min": 1, "max": 12},
        "VMO_B_INDK_AM_BIDRAG_BETAL": {"type": "float", "mean": 50000, "std": 20000},
        "VMO_B_INDK_AM_BIDRAG_FRI": {"type": "float", "mean": 10000, "std": 3000},
        "VMO_INDKOMST_ART_KODE": {"type": "choice", "options": ["0110", "0210", "0320", "0410"]},
        "VMO_INDKOMST_TYPE_KODE": {"type": "choice", "options": ["1", "2", "3", "4"]},
        "VMO_SLUTDATO": {
            "type": "date",
            "start": datetime(2009, 1, 1),
            "end": datetime(2022, 12, 31),
        },
        "VMO_STARTDATO": {
            "type": "date",
            "start": datetime(2009, 1, 1),
            "end": datetime(2022, 12, 31),
        },
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
    },
    "RAS": {
        "PNR": {"type": "pnr"},
        "ARBGNR": {"type": "int", "min": 1000000, "max": 9999999},
        "ARBNR": {"type": "int", "min": 1000, "max": 9999},
        "CVRNR": {"type": "int", "min": 10000000, "max": 99999999},
        "OK_NR": {"type": "int", "min": 100000, "max": 999999},
        "SENR": {"type": "int", "min": 10000000, "max": 99999999},
        "SOCSTIL_KODE": {
            "type": "choice",
            "options": ["110", "120", "130", "210", "220", "310", "320", "330"],
        },
        "SOC_STATUS_KODE": {"type": "choice", "options": ["1", "2", "3", "4", "5"]},
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
        "VERSION": {"type": "choice", "options": ["1", "2"]},
    },
    "SGDP": {
        "PNR": {"type": "pnr"},
        "ANTDAGE": {"type": "int", "min": 1, "max": 366},
        "ARBGHP": {"type": "float", "mean": 50000, "std": 10000},
        "ARBGIVNR": {"type": "int", "min": 1000000, "max": 9999999},
        "BERDAGE": {"type": "int", "min": 1, "max": 366},
        "FOERBER": {"type": "date", "start": datetime(2020, 1, 1), "end": datetime(2023, 12, 31)},
        "FOERFRAV": {"type": "date", "start": datetime(2020, 1, 1), "end": datetime(2023, 12, 31)},
        "FRAVDAGE": {"type": "int", "min": 1, "max": 366},
        "NEDDPPCT": {"type": "choice", "options": [25, 50, 75, 100]},
        "NEDTIM": {"type": "int", "min": 1, "max": 40},
        "OPHOERSAA": {"type": "choice", "options": ["1", "2", "3", "4", "5"]},
        "SAGSART": {"type": "choice", "options": ["1", "2", "3", "4", "5"]},
        "SIDBER": {"type": "date", "start": datetime(2020, 1, 1), "end": datetime(2023, 12, 31)},
        "SIKRHP": {"type": "float", "mean": 30000, "std": 5000},
        "STARTSAG": {"type": "choice", "options": ["1", "2", "3", "4", "5"]},
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
        "VERSION": {"type": "choice", "options": ["1", "2"]},
    },
    "LMDB": {
        "PNR12": {"type": "pnr"},
        "APK": {"type": "int", "min": 1, "max": 10},
        "ATC": {"type": "string", "prefix": "A", "min": 10, "max": 99},
        "DOSO": {"type": "choice", "options": ["1", "2", "3", "4"]},
        "EKSD": {"type": "date", "start": datetime(2000, 1, 1), "end": datetime(2023, 12, 31)},
        "INDO": {"type": "choice", "options": ["A", "B", "C", "D"]},
        "NAME": {"type": "string", "prefix": "DRUG_", "min": 0, "max": 999},
        "PACKSIZE": {"type": "int", "min": 10, "max": 100},
        "VOLUME": {"type": "float", "mean": 5, "std": 2},
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
    },
    "LPR_DIAG": {
        "RECNUM": {"type": "int", "min": 1000000, "max": 9999999},
        "C_DIAG": {"type": "string", "prefix": "D", "min": 10, "max": 99},
        "C_DIAGTYPE": {"type": "choice", "options": ["A", "B", "H"]},
        "C_TILDIAG": {"type": "string", "prefix": "T", "min": 10, "max": 99},
        "VERSION": {"type": "choice", "options": ["1", "2"]},
    },
    "LPR_SKSOPR": {
        "RECNUM": {"type": "int", "min": 1000000, "max": 9999999},
        "C_OPR": {"type": "string", "prefix": "O", "min": 10000, "max": 99999},
        "C_OPRART": {"type": "choice", "options": ["A", "B", "C"]},
        "C_TILOPR": {"type": "string", "prefix": "T", "min": 10, "max": 99},
        "D_ODTO": {"type": "date", "start": datetime(2000, 1, 1), "end": datetime(2023, 12, 31)},
        "VERSION": {"type": "choice", "options": ["1", "2"]},
    },
    "UDFK": {
        "PNR": {"type": "pnr"},
        "FAGKODE": {"type": "string", "prefix": "FAG", "min": 1000, "max": 9999},
        "KLASSETYPE": {"type": "choice", "options": ["A", "B", "C"]},
        "KLTRIN": {"type": "choice", "options": ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]},
        "SKOLEAAR": {"type": "choice", "options": ["2022/2023"]},
        "GRUNDSKOLEKARAKTER": {
            "type": "choice",
            "options": ["-3", "00", "02", "4", "7", "10", "12"],
        },
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
        "BEDOEMMELSESFORM": {"type": "choice", "options": ["1", "2", "3"]},
        "FAGDISCIPLIN": {"type": "string", "prefix": "FD", "min": 10, "max": 99},
        "GRUNDSKOLEFAG": {"type": "choice", "options": ["DAN", "MAT", "ENG", "FYS", "KEM"]},
        "GRUNDSKOLENIVEAU": {"type": "choice", "options": ["A", "B", "C", "D"]},
        "INSTNR": {"type": "int", "min": 100000, "max": 999999},
        "KARAKTERAARSAG": {"type": "choice", "options": ["1", "2", "3", "4"]},
        "KL_BETEGNELSE": {"type": "string", "prefix": "KL", "min": 10, "max": 99},
        "PROEVEART": {"type": "choice", "options": ["1", "2", "3"]},
        "PROEVEFORM": {"type": "choice", "options": ["A", "B", "C"]},
        "SKALA": {"type": "choice", "options": ["7", "13"]},
    },
    "LPR_ADM": {
        "PNR": {"type": "pnr"},
        "RECNUM": {"type": "int", "min": 1000000, "max": 9999999},
        "C_ADIAG": {"type": "string", "prefix": "D", "min": 10, "max": 99},
        "C_AFD": {"type": "int", "min": 1000, "max": 9999},
        "C_PATTYPE": {"type": "choice", "options": ["0", "1", "2"]},
        "C_SGH": {"type": "int", "min": 1000, "max": 9999},
        "C_SPEC": {"type": "int", "min": 10, "max": 99},
        "D_INDDTO": {"type": "date", "start": datetime(2000, 1, 1), "end": datetime(2019, 12, 31)},
        "D_UDDTO": {"type": "date", "start": datetime(2000, 1, 1), "end": datetime(2019, 12, 31)},
        "V_SENGDAGE": {"type": "int", "min": 1, "max": 30},
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
        "VERSION": {"type": "choice", "options": ["1", "2"]},
        "C_KONTAARS": {"type": "choice", "options": ["1", "2", "3", "4", "5"]},
        "C_HAFD": {"type": "int", "min": 1000, "max": 9999},
        "C_HENM": {"type": "choice", "options": ["1", "2", "3"]},
        "C_HSGH": {"type": "int", "min": 1000, "max": 9999},
        "C_INDM": {"type": "choice", "options": ["1", "2", "3"]},
        "C_KOM": {"type": "int", "min": 101, "max": 851},
        "C_UDM": {"type": "choice", "options": ["1", "2", "3", "4"]},
        "D_HENDTO": {"type": "date", "start": datetime(2000, 1, 1), "end": datetime(2019, 12, 31)},
        "K_AFD": {"type": "int", "min": 1000, "max": 9999},
        "V_ALDDG": {"type": "int", "min": 0, "max": 36500},
        "V_ALDER": {"type": "int", "min": 0, "max": 100},
    },
    "MFR": {
        "CPR_BARN": {"type": "pnr"},
        "CPR_MODER": {"type": "pnr"},
        "CPR_FADER": {"type": "pnr"},
        "FOEDSELSDATO": {
            "type": "date",
            "start": datetime(2000, 1, 1),
            "end": datetime(2018, 12, 31),
        },
        "GESTATIONSALDER_DAGE": {"type": "int", "min": 140, "max": 310},
        "GESTATIONSALDER_BARN": {"type": "int", "min": 20, "max": 45},  # Added this line
        "KOEN_BARN": {"type": "choice", "options": ["1", "2"]},
        "LAENGDE_BARN": {"type": "float", "mean": 50, "std": 3},
        "VAEGT_BARN": {"type": "float", "mean": 3500, "std": 500},
        "APGARSCORE_EFTER5MINUTTER": {"type": "int", "min": 0, "max": 10},
        "FLERFOLDSGRAVIDITET": {"type": "choice", "options": ["1", "2", "3", "4"]},
        "PK_MFR": {"type": "string", "prefix": "MFR", "min": 100000, "max": 999999},
        "FAMILIE_ID": {
            "type": "string",
            "prefix": "F",
            "min": 100000,
            "max": 999999,
        },  # Added this line
    },
    "IND": {
        "PNR": {"type": "pnr"},
        "BESKST13": {
            "type": "choice",
            "options": ["110", "120", "130", "210", "220", "310", "320", "330"],
        },
        "LOENMV_13": {"type": "float", "mean": 400000, "std": 100000},
        "PERINDKIALT_13": {"type": "float", "mean": 450000, "std": 120000},
        "ERHVERVSINDK_13": {"type": "float", "mean": 380000, "std": 90000},  # Added this line
        "PRE_SOCIO": {
            "type": "choice",
            "options": ["110", "120", "210", "220", "310", "320", "330"],
        },
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
        "VERSION": {"type": "choice", "options": ["1", "2"]},
    },
    "UDDF": {
        "PNR": {"type": "pnr"},
        "HFAUDD": {"type": "choice", "options": ["10", "20", "30", "35", "40", "50", "60", "70"]},
        "HF_KILDE": {"type": "choice", "options": ["UDD", "KVA", "GRU", "IVU"]},
        "HF_VFRA": {"type": "date", "start": datetime(1980, 1, 1), "end": datetime(2022, 12, 31)},
        "HF_VTIL": {
            "type": "date",
            "start": datetime(1980, 1, 1),
            "end": datetime(2030, 12, 31),
        },  # Added this line
        "INSTNR": {"type": "int", "min": 100000, "max": 999999},
        "CPRTJEK": {"type": "choice", "options": ["0", "1"]},
        "CPRTYPE": {"type": "choice", "options": ["0", "1", "2", "3"]},
        "VERSION": {"type": "choice", "options": ["1", "2"]},
    },
}

# Define the periods for each register
register_periods = {
    "BEF": list(range(2000, 2024)),  # 2000-2023
    "AKM": list(range(2000, 2023)),  # 2000-2022
    "FAIK": list(range(2000, 2022)),  # 2000-2021
    "IDAN": list(range(2000, 2022)),  # 2000-2021
    "ILME": list(range(2009, 2023)),  # 2009-2022
    "IND": list(range(2001, 2023)),  # 2001-2022
    "RAS": list(range(2000, 2022)),  # 2000-2021
    "LMDB": list(range(2000, 2024)),  # 2000-2023
    "LPR_ADM": list(range(2000, 2020)),  # 2000-2019
    "LPR_DIAG": list(range(2000, 2019)),  # 2000-2018
    "LPR_SKSOPR": list(range(2000, 2019)),  # 2000-2018
    "MFR": list(range(2000, 2019)),  # 2000-2018
    "DOD": list(range(2000, 2023)),  # 2000-2022
    "DODSAARS": list(range(2000, 2002)),  # 2000-2001
    "DODSAASG": list(range(2000, 2022)),  # 2000-2021
    "UDDF": [2022],  # Only 2022
    "UDFK": [2023],  # Only 2023
    "VNDS": [2022],  # Only 2022
    "SGDP": list(range(2000, 2020)),  # 2000-2019
}


def data_exists(file_path):
    return os.path.exists(file_path)


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic data for registers.")
    parser.add_argument("--force", action="store_true", help="Force regeneration of all data")
    parser.add_argument("--registers", nargs="+", help="Specific registers to process")
    parser.add_argument("--years", nargs="+", type=int, help="Specific years to process")
    args = parser.parse_args()

    base_dir = "synth_data"
    os.makedirs(base_dir, exist_ok=True)

    registers_to_process = args.registers if args.registers else register_configs.keys()

    for register in registers_to_process:
        if register not in register_configs:
            logger.warning(f"Configuration for register '{register}' not found. Skipping.")
            continue

        config = register_configs[register]
        register_dir = os.path.join(base_dir, register.lower())
        os.makedirs(register_dir, exist_ok=True)

        periods = register_periods.get(register, [2022])  # Default to 2022 if not specified
        years_to_process = args.years if args.years else periods

        for year in years_to_process:
            if year not in periods:
                logger.warning(
                    f"Year {year} is not in the specified periods for register '{register}'. Skipping."
                )
                continue

            file_name = f"{register.lower()}_{year}.parquet"
            file_path = os.path.join(register_dir, file_name)

            if args.force or not data_exists(file_path):
                num_records = 1000  # You can adjust this for each register and year if needed
                year_data = generate_data(config, num_records, year)

                # Save the data to a parquet file
                year_data.write_parquet(file_path)

                logger.info(f"Generated and saved {file_name}")

            else:
                logger.info(f"Data for {register} {year} already exists. Skipping.")

        logger.info(f"Completed processing for {register}")

    logger.info("Data generation complete.")


if __name__ == "__main__":
    main()
