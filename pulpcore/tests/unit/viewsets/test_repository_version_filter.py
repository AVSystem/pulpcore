from uuid import uuid4

import pytest
from django.contrib.auth import get_user_model
from rest_framework import serializers
from rest_framework.test import APIClient

from pulpcore.app import models
from pulpcore.app.contexts import _current_domain, _current_user_func, current_pulp_api_version
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


def malformed_hrefs(versions):
    """Values that are not hrefs of a repository version, keyed by what is wrong with them."""
    good = version_href(versions["a1"])
    repository = get_url(versions["a1"].repository)
    api_root = repository.split("repositories/")[0]
    return {
        "non-numeric version number": good.replace("/versions/1/", "/versions/abc/"),
        "negative version number": good.replace("/versions/1/", "/versions/-1/"),
        "non-ascii digits": good.replace("/versions/1/", "/versions/\u0661/"),
        "repository segment is not a uuid": good.replace(
            str(versions["a1"].repository.pk), "not-a-uuid"
        ),
        "href of a repository": repository,
        "href of another model": f"{api_root}content/file/files/{uuid4()}/",
        "href of a route below a version": f"{good}repair/",
        "unknown route": "/pulp/api/v3/no/such/route/",
        "not a path": "not-an-href",
        "prn of another model": f"prn:core.repository:{uuid4()}",
        "prn of a model that does not exist": f"prn:core.nothing:{uuid4()}",
        "prn with a pk that is not a uuid": "prn:core.repositoryversion:not-a-uuid",
        "prn without a pk": "prn:core.repositoryversion",
    }


MALFORMED = [
    "non-numeric version number",
    "negative version number",
    "non-ascii digits",
    "repository segment is not a uuid",
    "href of a repository",
    "href of another model",
    "href of a route below a version",
    "unknown route",
    "not a path",
    "prn of another model",
    "prn of a model that does not exist",
    "prn with a pk that is not a uuid",
    "prn without a pk",
]


@pytest.mark.parametrize("kind", MALFORMED)
def test_pulp_href_in_rejects_malformed_values(versions, kind):
    value = malformed_hrefs(versions)[kind]

    with pytest.raises(serializers.ValidationError):
        filtered(value)


def test_pulp_href_in_rejects_a_malformed_value_among_valid_ones(versions):
    value = ",".join([version_href(versions["a1"]), "not-an-href"])

    with pytest.raises(serializers.ValidationError):
        filtered(value)


@pytest.fixture
def admin_client(db):
    """An API client logged in as an administrator, leaving no request context behind."""
    contexts = (_current_user_func, _current_domain, current_pulp_api_version)
    tokens = [(var, var.set(var.get())) for var in contexts]
    client = APIClient()
    client.force_authenticate(get_user_model().objects.create_superuser(f"admin-{uuid4()}"))
    yield client
    for var, token in tokens:
        var.reset(token)


@pytest.mark.parametrize("kind", MALFORMED)
def test_list_request_answers_400_for_malformed_values(admin_client, versions, kind):
    prefix = get_url(versions["a1"].repository).split("repositories/")[0]

    response = admin_client.get(
        f"{prefix}repository_versions/", {"pulp_href__in": malformed_hrefs(versions)[kind]}
    )

    assert response.status_code == 400


def test_list_request_filters_by_version_href(admin_client, versions):
    prefix = get_url(versions["a1"].repository).split("repositories/")[0]

    response = admin_client.get(
        f"{prefix}repository_versions/", {"pulp_href__in": version_href(versions["a1"])}
    )

    assert response.status_code == 200
    assert [row["pulp_href"] for row in response.json()["results"]] == [
        version_href(versions["a1"])
    ]
