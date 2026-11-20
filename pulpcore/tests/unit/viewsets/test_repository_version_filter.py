import pytest

from pulpcore.app import models
from pulpcore.app.util import get_prn, get_url
from pulpcore.app.viewsets.repository import RepositoryVersionFilter

from pulp_file.app.models import FileRepository


def version_href(version):
    return f"{get_url(version.repository)}versions/{version.number}/"


@pytest.fixture
def versions(db):
    """The initial and one further complete version of two repositories."""
    created = {}
    for label in ("a", "b"):
        repo = FileRepository.objects.create(name=f"version-filter-{label}")
        created[f"{label}0"] = repo.versions.get(number=0)
        created[f"{label}1"] = models.RepositoryVersion.objects.create(
            repository=repo, number=1, complete=True
        )
    return created


def filtered(value):
    """Apply the filter set the way a list request does, for one pulp_href__in value."""
    data = {} if value is None else {"pulp_href__in": value}
    queryset = models.RepositoryVersion.objects.all()
    return RepositoryVersionFilter(data=data, queryset=queryset).qs


@pytest.mark.django_db
def test_pulp_href_in_selects_versions_of_different_repositories(versions):
    value = ",".join(version_href(versions[key]) for key in ("a1", "b0"))

    assert set(filtered(value)) == {versions["a1"], versions["b0"]}


@pytest.mark.django_db
def test_pulp_href_in_accepts_prns(versions):
    value = ",".join(get_prn(versions[key]) for key in ("a0", "b1"))

    assert set(filtered(value)) == {versions["a0"], versions["b1"]}


@pytest.mark.django_db
def test_pulp_href_in_accepts_hrefs_and_prns_together(versions):
    value = ",".join([version_href(versions["a1"]), get_prn(versions["b1"])])

    assert set(filtered(value)) == {versions["a1"], versions["b1"]}


@pytest.mark.django_db
def test_pulp_href_in_not_given_filters_nothing(versions):
    assert set(filtered(None)) == set(models.RepositoryVersion.objects.all())


@pytest.mark.django_db
def test_pulp_href_in_empty_list_selects_nothing(versions):
    assert list(filtered("")) == []


@pytest.mark.django_db
def test_pulp_href_in_with_unknown_version_returns_no_rows(versions):
    unknown = version_href(versions["a1"]).replace("/versions/1/", "/versions/99/")

    assert unknown.endswith("/versions/99/")
    assert list(filtered(unknown)) == []
