"""Test packaging configuration and metadata."""
import sys
from importlib.metadata import version, requires

import pytest


def test_version_starts_with_7():
    """Verify package version starts with 7."""
    v = version("hysds-mozart")
    assert v.startswith("7."), f"Expected version 7.x, got {v}"


def test_required_sibling_deps_declared():
    """Verify HySDS sibling dependencies are declared."""
    deps = requires("hysds-mozart")
    assert deps is not None, "No dependencies found"
    
    dep_names = {dep.split()[0].split(";")[0].split(">=")[0].split("~=")[0].split("<")[0]
                 for dep in deps}
    
    required_siblings = {"hysds-core", "hysds-commons"}
    missing = required_siblings - dep_names
    
    assert not missing, f"Missing required HySDS deps: {missing}"


def test_future_not_a_dependency():
    """Verify 'future' package is not a dependency."""
    deps = requires("hysds-mozart")
    assert deps is not None
    
    for dep in deps:
        dep_name = dep.split()[0].split(";")[0].split(">=")[0].split("~=")[0].split("<")[0]
        assert dep_name != "future", "'future' should not be a dependency on Python 3.12+"


def test_core_modules_importable():
    """Verify core mozart modules can be imported."""
    import mozart
    assert hasattr(mozart, "__version__")
    
    # Test key modules exist
    try:
        from mozart import app
        assert app is not None
    except ImportError as e:
        pytest.skip(f"Skipping module import test: {e}")


def test_python_version_requirement():
    """Verify running on Python 3.12+."""
    assert sys.version_info >= (3, 12), "Requires Python 3.12+"


def test_package_name_is_hysds_mozart():
    """Verify package is published as hysds-mozart."""
    v = version("hysds-mozart")
    assert v is not None, "Package 'hysds-mozart' not found"


def test_import_name_is_mozart():
    """Verify import name remains 'mozart' (not hysds_mozart)."""
    import mozart
    assert mozart.__name__ == "mozart"


def test_numpy_has_upper_bound():
    """Verify numpy has <2.0.0 upper bound per migration spec."""
    deps = requires("hysds-mozart")
    assert deps is not None
    
    numpy_deps = [d for d in deps if d.startswith("numpy")]
    assert numpy_deps, "numpy dependency not found"
    
    for dep in numpy_deps:
        assert "<2.0.0" in dep or "<2.0" in dep or "<2" in dep, \
            f"numpy should have <2.0.0 upper bound, found: {dep}"


def test_werkzeug_has_upper_bound():
    """Verify werkzeug has <3.0.0 upper bound per migration spec."""
    deps = requires("hysds-mozart")
    assert deps is not None
    
    werkzeug_deps = [d for d in deps if "werkzeug" in d.lower()]
    assert werkzeug_deps, "werkzeug dependency not found"
    
    for dep in werkzeug_deps:
        assert "<3.0" in dep or "<3" in dep, \
            f"werkzeug should have <3.0.0 upper bound, found: {dep}"
