"""
Unit tests for ElastiCache Valkey engine support.

These test the engine → image/port mapping and the no-Docker fallback
directly — no running server or real Docker needed.
"""

from ministack.services import elasticache
from ministack.services.elasticache import _engine_image_and_port


def _img(name):
    return elasticache.apply_image_prefix(name)


def test_valkey_image_and_port():
    assert _engine_image_and_port("valkey", "8.0") == (_img("valkey/valkey:8.0-alpine"), 6379)
    assert _engine_image_and_port("valkey", "7.2") == (_img("valkey/valkey:7.2-alpine"), 6379)
    assert _engine_image_and_port("valkey", "8.1") == (_img("valkey/valkey:8.1-alpine"), 6379)


def test_valkey_image_tag_truncates_patch_version():
    assert _engine_image_and_port("valkey", "7.2.6") == (_img("valkey/valkey:7.2-alpine"), 6379)


def test_valkey_image_tag_defaults():
    assert _engine_image_and_port("valkey", "8") == (_img("valkey/valkey:8-alpine"), 6379)
    assert _engine_image_and_port("valkey", "") == (_img("valkey/valkey:8.0-alpine"), 6379)


def test_redis_and_memcached_images_unchanged():
    assert _engine_image_and_port("redis", "7.1.0") == (_img("redis:7-alpine"), 6379)
    assert _engine_image_and_port("memcached", "1.6.17") == (_img("memcached:1.6.17-alpine"), 11211)


def test_valkey_no_docker_fallback_uses_redis_port(monkeypatch):
    """Valkey previously fell into the memcached branch: nonexistent
    memcached:<ver>-alpine image, then a fallback advertising port 11211."""
    monkeypatch.setattr(elasticache, "_get_docker", lambda: None)
    host, port, cid = elasticache._spawn_redis_container(
        "ms-valkey-test", "valkey", "8.0", {"ministack": "elasticache"}
    )
    assert (host, port) == (elasticache.REDIS_DEFAULT_HOST, elasticache.REDIS_DEFAULT_PORT)
    assert cid is None
