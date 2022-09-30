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
        Path(f'{mod.EXAMPLE_DATA_DIR}/filers.json').read_text(encoding='utf8')
    )
    def get_filer(filer_nid):
        filer = [ f for f in filers if f['filerNid'] == str(filer_nid) ]
        return filer[:1]

    monkeypatch.setattr(mod, 'get_filer', get_filer)
    return filers

@pytest.fixture
def stub_get_trans(monkeypatch):
    trans = json.loads(
        Path(f'{mod.EXAMPLE_DATA_DIR}/transactions.json').read_text(encoding='utf8')
    )

    monkeypatch.setattr(mod, 'get_trans', lambda: trans)
    return trans

@pytest.fixture
def output_test_data(monkeypatch):
    test_output_path = Path('test_output')
    if not test_output_path.exists() or not test_output_path.is_dir():
        test_output_path.mkdir()

    monkeypatch.setattr(mod, 'OUTPUT_DATA_DIR', test_output_path.name)

@pytest.fixture
def save_source_data(monkeypatch, tmp_path):
    example_data_dir = str(tmp_path.resolve())
    monkeypatch.setattr(mod, 'EXAMPLE_DATA_DIR', example_data_dir)

def test_main(stub_get_filings, stub_get_filer, stub_get_trans, output_test_data, save_source_data):
    mod.main(*mod.load_source_data())
