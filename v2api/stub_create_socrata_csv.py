import json
from pathlib import Path
import pytest
from . import create_socrata_csv as mod

@pytest.fixture
def stub_get_filings(monkeypatch):
    filings = json.loads(Path(f'{mod.EXAMPLE_DATA_DIR}/filings.json').read_text(encoding='utf8'))
    def get_filings():
        return filings, {
            'next_offset': None,
            'total': len(filings)
        }

    monkeypatch.setattr(mod, 'get_filings', get_filings)
    return filings

@pytest.fixture
def stub_get_filer(monkeypatch):
    filers = json.loads(
        Path(f'{mod.EXAMPLE_DATA_DIR}/filers_2019-present.json').read_text(encoding='utf8')
    )
    def get_filer(filer_nid):
        filer = [ f for f in filers if f['filerNid'] == str(filer_nid) ]
        return filer[:1]

    monkeypatch.setattr(mod, 'get_filer', get_filer)
    return filers

@pytest.fixture
def stub_get_trans(monkeypatch):
    trans = json.loads(
        Path(f'{mod.EXAMPLE_DATA_DIR}/transactions_2019-present.json').read_text(encoding='utf8')
    )
    def get_transactions(filing_nid):
        return [ t for t in trans if t['filingNid'] == str(filing_nid) ]

    monkeypatch.setattr(mod, 'get_all_trans_for_filing', get_transactions)
    return trans

def test_main(stub_get_filings, stub_get_filer, stub_get_trans):
    mod.main()
