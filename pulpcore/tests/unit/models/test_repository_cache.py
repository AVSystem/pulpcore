import hashlib
from unittest import mock
from uuid import uuid4

import pytest

from pulpcore.app.models import Content
from pulpcore.app.util import cache_key

from pulp_file.app.models import FileContent, FileDistribution, FileRepository


@pytest.fixture
def distributed_repository(db, settings):
    settings.CACHE_ENABLED = True
    repository = FileRepository.objects.create(name=f"repo-{uuid4().hex[:8]}")
    name = f"dist-{uuid4().hex[:8]}"
    FileDistribution.objects.create(name=name, base_path=name, repository=repository)
    return repository, name


def add_file(version, path):
    digest = hashlib.sha256(path.encode()).hexdigest()
    content = FileContent.objects.create(relative_path=path, digest=digest)
    version.add_content(Content.objects.filter(pk=content.pk))


def test_cache_invalidated_once_new_version_is_complete(
    distributed_repository, django_capture_on_commit_callbacks
):
    """
    While a new version is being created, the distribution is still served from the previous
    one, and a response computed then can be put into the cache. Clearing the cache only when
    the version is started would leave that response there until it expires, so the cache must
    be cleared once the new version is complete.
    """
    repository, base_path = distributed_repository
    complete_at_invalidation = []

    def record(*args, base_key=None, **kwargs):
        if base_key is not None and cache_key(base_path) in base_key:
            complete_at_invalidation.append(repository.versions.latest("number").complete)

    with mock.patch("pulpcore.app.models.repository.Cache") as cache:
        cache.return_value.delete.side_effect = record
        with django_capture_on_commit_callbacks(execute=True):
            with repository.new_version() as version:
                add_file(version, "a.txt")

    assert version.complete
    assert True in complete_at_invalidation


def test_cache_not_invalidated_by_a_version_without_changes(
    distributed_repository, django_capture_on_commit_callbacks
):
    """A new version that changes nothing is discarded and leaves the cache alone."""
    repository, _ = distributed_repository

    with mock.patch("pulpcore.app.models.repository.Cache") as cache:
        with django_capture_on_commit_callbacks(execute=True):
            with repository.new_version():
                pass

    cache.return_value.delete.assert_not_called()
