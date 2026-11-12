import asyncio
import datetime
import hashlib
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from gettext import gettext as _
from logging import getLogger

from asgiref.sync import sync_to_async
from django.conf import settings
from django.db import connection, transaction
from django.utils import timezone

from pulpcore.app import models
from pulpcore.app.models import ProgressReport
from pulpcore.app.redis_connection import get_redis_connection
from pulpcore.app.util import get_domain, get_prn
from pulpcore.constants import TASK_STATES
from pulpcore.exceptions.base import RepositoryVersionDeleteError

log = getLogger(__name__)


CHUNK_SIZE = 1024 * 1024  # 1 Mb


def delete_version(pk, **kwargs):
    """
    Delete a repository version by squashing its changes with the next newer version. This ensures
    that the content set for each version stays the same.

    There must be a newer version to squash into. If we deleted the latest version, the next content
    change would create a new one of the same number, which would violate the immutability
    guarantee.

    Args:
        pk (uuid): the primary key for a RepositoryVersion to delete

    Raises:
        models.RepositoryVersion.DoesNotExist: if there is not a newer version to squash into.
            TODO: something more friendly
        ValidationError: if there's one repo version
    """
    with transaction.atomic():
        try:
            version = models.RepositoryVersion.objects.get(pk=pk)
        except models.RepositoryVersion.DoesNotExist:
            log.info(_("The repository version was not found. Nothing to do."))
            return

        if version.repository.versions.complete().count() <= 1:
            raise RepositoryVersionDeleteError()

        log.info(
            "Deleting and squashing version {num} of repository '{repo}'".format(
                num=version.number, repo=version.repository.name
            )
        )

        version.delete()


async def _repair_ca(content_artifact, repaired=None):
    remote_artifacts = content_artifact.remoteartifact_set.all().select_related("remote")

    if not await remote_artifacts.aexists():
        log.warning(
            "Artifact {} is unrepairable - no remote source".format(content_artifact.artifact)
        )
        log.warning(
            "Deleting file for the unrepairable artifact {}".format(content_artifact.artifact)
        )
        await sync_to_async(content_artifact.artifact.file.delete)(save=False)
        return False

    async for remote_artifact in remote_artifacts:
        detail_remote = await remote_artifact.remote.acast()
        downloader = detail_remote.get_downloader(remote_artifact)
        try:
            dl_result = await downloader.run()
        except Exception as e:
            log.warning(_("Redownload failed from '{}': {}.").format(remote_artifact.url, str(e)))
        else:
            if dl_result.artifact_attributes["sha256"] == content_artifact.artifact.sha256:
                with open(dl_result.path, "rb") as src:
                    filename = content_artifact.artifact.file.name
                    await sync_to_async(content_artifact.artifact.file.delete)(save=False)
                    await sync_to_async(content_artifact.artifact.file.save)(
                        filename, src, save=False
                    )
                if repaired is not None:
                    await repaired.aincrement()
                return True
    log.warning("Deleting file for the unrepairable artifact {}".format(content_artifact.artifact))
    await sync_to_async(content_artifact.artifact.file.delete)(save=False)
    return False


def _verify_artifact(artifact):
    domain = get_domain()
    assert domain.pk == artifact.pulp_domain_id
    try:
        # verify files digest
        hasher = hashlib.sha256()
        with artifact.file as fp:
            for chunk in fp.chunks(CHUNK_SIZE):
                hasher.update(chunk)
        return hasher.hexdigest() == artifact.sha256
    except FileNotFoundError:
        return False


async def _repair_artifacts_for_content(subset=None, verify_checksums=True):
    loop = asyncio.get_event_loop()
    pending = set()

    query_set = models.ContentArtifact.objects.exclude(artifact__isnull=True)

    domain = get_domain()

    if subset is None or not await subset.aexists():
        subset = models.Content.objects.filter(pulp_domain=domain)

    query_set = query_set.filter(content__in=subset.values("pk"))

    async with (
        ProgressReport(message="Identify missing units", code="repair.missing") as missing,
        ProgressReport(message="Identify corrupted units", code="repair.corrupted") as corrupted,
        ProgressReport(message="Repair corrupted units", code="repair.repaired") as repaired,
    ):
        with ThreadPoolExecutor(max_workers=2) as checksum_executor:
            storage = domain.get_storage()
            async for content_artifact in query_set.select_related("artifact").aiterator():
                artifact = content_artifact.artifact

                valid = await loop.run_in_executor(None, storage.exists, artifact.file.name)
                if not valid:
                    await missing.aincrement()
                    log.warning(_("Missing file for {}").format(artifact))
                elif verify_checksums:
                    # default ThreadPoolExecutor uses num cores x 5 threads. Since we're doing
                    # such long and sequential reads, using too many threads might hurt more
                    # than help (on HDDs, maybe not on SSDs) by making the disk access pattern
                    # more random. Put it in a separate executor with limited threads.
                    # Should stay in (an) executor so that at least it doesn't completely block
                    # downloads.
                    valid = await loop.run_in_executor(
                        checksum_executor, copy_context().run, _verify_artifact, artifact
                    )
                    if not valid:
                        await corrupted.aincrement()
                        log.warning(_("Digest mismatch for {}").format(artifact))

                if not valid:
                    if len(pending) >= 5:  # Limit the number of concurrent repair tasks
                        done, pending = await asyncio.wait(
                            pending, return_when=asyncio.FIRST_COMPLETED
                        )
                        await asyncio.gather(*done)  # Clean up tasks
                    pending.add(asyncio.ensure_future(_repair_ca(content_artifact, repaired)))
        await asyncio.gather(*pending)


def repair_version(repository_version_pk, verify_checksums, **kwargs):
    """
    Repair the artifacts associated with this repository version.

    Artifact files that have suffered from bit rot, were altered or have gone missing will be
    attempted to be refetched fron an associated upstream.

    Args:
        repository_version_pk (uuid): the primary key for a RepositoryVersion to delete
        verify_checksums (bool): whether to calculate and compare checksums for all artifacts
    """

    version = models.RepositoryVersion.objects.get(pk=repository_version_pk)

    log.info(
        "Repairing version {num} of repository '{repo}'".format(
            num=version.number, repo=version.repository.name
        ),
    )

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        _repair_artifacts_for_content(subset=version.content, verify_checksums=verify_checksums)
    )


def repair_all_artifacts(verify_checksums, **kwargs):
    """
    Repair all artifacts, globally.

    Artifact files that have suffered from bit rot, were altered or have gone missing will be
    attempted to be refetched fron an associated upstream.

    Args:
        verify_checksums (bool): whether to calculate and compare checksums for all artifacts
    """
    log.info(_("Repairing artifacts.'"))

    loop = asyncio.get_event_loop()
    loop.run_until_complete(_repair_artifacts_for_content(verify_checksums=verify_checksums))


def add_and_remove(
    repository_pk,
    add_content_units,
    remove_content_units,
    base_version_pk=None,
    overwrite=True,
    **kwargs,
):
    """
    Create a new repository version by adding and then removing content units.

    Args:
        repository_pk (uuid): The primary key for a Repository for which a new Repository Version
            should be created.
        add_content_units (list): List of PKs for [pulpcore.app.models.Content][] that
            should be added to the previous Repository Version for this Repository.
        remove_content_units (list): List of PKs for[pulpcore.app.models.Content][] that
            should be removed from the previous Repository Version for this Repository.
        base_version_pk (uuid): the primary key for a RepositoryVersion whose content will be used
            as the initial set of content for our new RepositoryVersion
        overwrite (bool): When False, raise ContentOverwriteError if any content being added
            conflicts with existing content based on repo_key_fields. Defaults to True.
    """
    repository = models.Repository.objects.get(pk=repository_pk).cast()

    if base_version_pk:
        base_version = models.RepositoryVersion.objects.get(pk=base_version_pk)
    else:
        base_version = None

    if "*" in remove_content_units:
        latest = repository.latest_version()
        if latest:
            remove_content_units = latest.content.values_list("pk", flat=True)
        else:
            remove_content_units = []

    if not overwrite and add_content_units:
        version = base_version or repository.latest_version()
        if version:
            remove_pks = list(remove_content_units) if remove_content_units else None
            repository.check_content_overwrite(
                version, add_content_units, remove_content_pks=remove_pks
            )

    with repository.new_version(base_version=base_version) as new_version:
        new_version.remove_content(models.Content.objects.filter(pk__in=remove_content_units))
        new_version.add_content(models.Content.objects.filter(pk__in=add_content_units))


async def aadd_and_remove(
    repository_pk, add_content_units, remove_content_units, base_version_pk=None, **kwargs
):
    """Aynsc version of add_and_remove."""
    repository = await models.Repository.objects.aget(pk=repository_pk)
    repository = await repository.acast()

    if base_version_pk:
        base_version = await models.RepositoryVersion.objects.aget(pk=base_version_pk)
    else:
        base_version = None

    if "*" in remove_content_units:
        latest = await repository.alatest_version()
        if latest:
            remove_content_units = latest.content.values_list("pk", flat=True)
        else:
            remove_content_units = []

    def add_to_repository():
        with repository.new_version(base_version=base_version) as new_version:
            new_version.remove_content(models.Content.objects.filter(pk__in=remove_content_units))
            new_version.add_content(models.Content.objects.filter(pk__in=add_content_units))

    await sync_to_async(add_to_repository)()


PULL_THROUGH_PENDING_KEY = "pulp:pull-through-pending:{pk}"
PULL_THROUGH_SCHEDULE = "Add the content pulled through into repository {pk}"


def batch_pull_through():
    """Whether content fetched through a remote is added in batches (PULL_THROUGH_BATCH_WINDOW)."""
    return bool(settings.PULL_THROUGH_BATCH_WINDOW) and get_redis_connection() is not None


def _pull_through_lock_key(repository_pk):
    """A Postgres advisory lock key for scheduling a repository's pull-through commit."""
    key = f"pull-through-commit:{repository_pk}"
    return int(hashlib.sha256(key.encode()).hexdigest()[:15], 16)


def add_pull_through_pending(repository, content_pk):
    """Record content fetched through a remote for the repository's next pull-through batch.

    The content waits in a Redis set per repository; until it is committed, the content app
    serves it from storage through its remote artifact. Only content new to the set touches
    the database, to make sure the commit is scheduled.
    """
    key = PULL_THROUGH_PENDING_KEY.format(pk=repository.pk)
    if get_redis_connection().sadd(key, str(content_pk)):
        schedule_pull_through_commit(repository.pk, repository.pulp_domain_id)


def schedule_pull_through_commit(repository_pk, domain_pk):
    """Make sure a repository's pending pull-through content is committed within the window.

    Each repository has a one-shot task schedule. The first content of a window sets it to
    fire PULL_THROUGH_BATCH_WINDOW seconds later, and content arriving before it fires is
    committed with that one. Later content does not move it, so a steady stream still gets a
    version every window. The schedule waits in the database, so no worker is held for the
    length of the window.
    """
    name = PULL_THROUGH_SCHEDULE.format(pk=repository_pk)
    due = timezone.now() + datetime.timedelta(seconds=settings.PULL_THROUGH_BATCH_WINDOW)
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(%s)", [_pull_through_lock_key(repository_pk)]
            )
        schedule = models.TaskSchedule.objects.filter(name=name, pulp_domain_id=domain_pk).first()
        if schedule is None:
            models.TaskSchedule.objects.create(
                name=name,
                task_name=f"{dispatch_pull_through_commit.__module__}."
                f"{dispatch_pull_through_commit.__name__}",
                task_kwargs={"repository_pk": str(repository_pk)},
                next_dispatch=due,
                dispatch_interval=None,
                pulp_domain_id=domain_pk,
            )
        elif schedule.next_dispatch is None:
            schedule.next_dispatch = due
            schedule.save(update_fields=["next_dispatch"])


def dispatch_pull_through_commit(repository_pk):
    """Dispatch the commit of a repository's pending pull-through content.

    Run by the repository's task schedule, which cannot reserve the repository itself. The
    commit task does, and is dispatched only if none is already waiting to run.
    """
    from pulpcore.tasking.tasks import dispatch

    repository = models.Repository.objects.get(pk=repository_pk).cast()
    with transaction.atomic():
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(%s)", [_pull_through_lock_key(repository_pk)]
            )
        commit_waiting = models.Task.objects.filter(
            name=f"{commit_pull_through.__module__}.{commit_pull_through.__name__}",
            state=TASK_STATES.WAITING,
            reserved_resources_record__contains=[get_prn(repository)],
        ).exists()
        if not commit_waiting:
            dispatch(
                commit_pull_through,
                exclusive_resources=[repository],
                kwargs={"repository_pk": str(repository_pk)},
            )


def commit_pull_through(repository_pk):
    """Add all pending pull-through content of a repository in one repository version.

    Content fetched while it runs stays pending, and the repository's schedule is set again for
    it. If adding fails, the content taken is put back, so the next commit tries it again.
    """
    redis = get_redis_connection()
    key = PULL_THROUGH_PENDING_KEY.format(pk=repository_pk)
    pending = redis.spop(key, redis.scard(key)) or []
    content_pks = [pk.decode() if isinstance(pk, bytes) else pk for pk in pending]
    if content_pks:
        try:
            add_and_remove(repository_pk, add_content_units=content_pks, remove_content_units=[])
        except Exception:
            redis.sadd(key, *content_pks)
            raise
        log.info(
            "Committed pending pull-through content: repository=%s, units=%d",
            repository_pk,
            len(content_pks),
        )
    if redis.scard(key):
        repository = models.Repository.objects.get(pk=repository_pk)
        schedule_pull_through_commit(repository_pk, repository.pulp_domain_id)
