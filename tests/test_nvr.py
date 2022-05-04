from pytest import raises

from flatpak_indexer.nvr import NVR


def test_nvr():
    nvr = NVR("abc-libs-1.2-1.fc30")

    # NVR is idempotent
    assert NVR(nvr) is nvr

    assert nvr.name == "abc-libs"
    assert nvr.version == "1.2"
    assert nvr.release == "1.fc30"

    assert str(nvr) == "abc-libs-1.2-1.fc30"
    assert repr(nvr) == "NVR('abc-libs-1.2-1.fc30')"


def test_nvr_ordering():
    assert "abc-1.2-1.fc30" > "abc+-1.2-1.fc30"
    assert NVR("abc-1.2-1.fc30") < NVR("abc+-1.2-1.fc30")
    assert NVR("abc+-1.2-1.fc30") > NVR("abc-1.2-1.fc30")

    assert NVR("abc-1.2-1.fc30") <= NVR("abc-1.2-1.fc30")
    assert NVR("abc-1.2-1.fc30") >= NVR("abc-1.2-1.fc30")

    assert "abc-112-1" < "abc-12-1"
    assert NVR("abc-112-1") > NVR("abc-12-1")
    assert NVR("abc-12-1") < NVR("abc-112-1")


def test_nvr_invalid():
    with raises(ValueError, match=r"Argument to NVR\(\) must have at least two dashes"):
        NVR("abc-1.2")
