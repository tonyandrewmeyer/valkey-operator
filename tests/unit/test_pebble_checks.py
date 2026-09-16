#!/usr/bin/env python3
# Copyright 2026 Canonical Ltd.
# See LICENSE file for licensing details.

"""Tests for the Pebble health checks on the K8s substrate."""

from unittest.mock import patch

import ops
import pytest
from ops import testing

from src.charm import ValkeyCharm
from src.literals import (
    CONTAINER,
    HEALTH_CHECK_USER,
    PEER_RELATION,
    SENTINEL_HEALTH_CHECK,
    SENTINEL_TLS_PORT,
    STATUS_PEERS_RELATION,
    TLS_PORT,
    VALKEY_HEALTH_CHECK,
    CharmUsers,
    StartState,
)
from src.workload_k8s import ValkeyK8sWorkload

ENDPOINT = "valkey-0.valkey-endpoints"


def _workload() -> ValkeyK8sWorkload:
    ctx = testing.Context(ValkeyCharm)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state = testing.State(containers={container})
    with ctx(ctx.on.update_status(), state) as manager:
        return ValkeyK8sWorkload(container=manager.charm.unit.get_container(CONTAINER))


def test_pebble_layer_has_a_check_per_service():
    layer = _workload().build_pebble_layer(health_check_host=ENDPOINT).to_dict()

    assert set(layer["checks"]) == {VALKEY_HEALTH_CHECK, SENTINEL_HEALTH_CHECK}
    assert layer["services"]["valkey"]["on-check-failure"] == {VALKEY_HEALTH_CHECK: "restart"}
    assert layer["services"]["sentinel"]["on-check-failure"] == {SENTINEL_HEALTH_CHECK: "restart"}


def test_health_checks_have_no_level():
    """A level=alive check would hand restarts to Kubernetes, not Pebble.

    Valkey can spend a long time loading an RDB at startup, which is exactly
    the case the ops how-to says must not carry a liveness level.
    """
    layer = _workload().build_pebble_layer(health_check_host=ENDPOINT).to_dict()

    for check in layer["checks"].values():
        assert "level" not in check


@pytest.mark.parametrize(
    ("check_name", "port"),
    [(VALKEY_HEALTH_CHECK, TLS_PORT), (SENTINEL_HEALTH_CHECK, SENTINEL_TLS_PORT)],
)
def test_health_check_pings_the_tls_port_as_the_health_check_user(check_name, port):
    layer = _workload().build_pebble_layer(health_check_host=ENDPOINT).to_dict()

    command = layer["checks"][check_name]["exec"]["command"]
    assert command.startswith("valkey-cli ")
    assert command.endswith(" ping")
    assert f"-p {port}" in command
    assert f"-h {ENDPOINT}" in command
    assert f"--user {HEALTH_CHECK_USER}" in command
    # The TLS port is always listening (tls-replication is always on), so the
    # command does not depend on whether client TLS is enabled.
    assert "--tls" in command


def test_health_checks_carry_no_credentials():
    """The health-check user is nopass, so nothing secret reaches the plan."""
    layer = _workload().build_pebble_layer(health_check_host=ENDPOINT).to_dict()

    for check in layer["checks"].values():
        assert "environment" not in check["exec"]


def test_pebble_layer_without_a_host_has_no_checks():
    """Before the peer databag names this unit there is nothing to ping."""
    layer = _workload().build_pebble_layer(health_check_host="").to_dict()

    assert "checks" not in layer
    assert "on-check-failure" not in layer["services"]["valkey"]


def test_acl_file_grants_the_health_check_user_ping_only():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    relation = testing.PeerRelation(id=1, endpoint=PEER_RELATION)
    container = testing.Container(name=CONTAINER, can_connect=True)
    state_in = testing.State(leader=True, relations={relation}, containers={container})
    passwords = {user.value: "hunter2" for user in CharmUsers}

    with ctx(ctx.on.update_status(), state_in) as manager:
        with patch("workload_k8s.ValkeyK8sWorkload.write_file") as write_file:
            manager.charm.auth_manager.set_acl_file(passwords=passwords)
            manager.charm.auth_manager.set_sentinel_acl_file(passwords=passwords)

    line = f"user {HEALTH_CHECK_USER} on nopass -@all +ping\n"
    assert all(line in call.args[0] for call in write_file.call_args_list)


def _state_with_check(info: testing.CheckInfo, unit_data: dict[str, str] | None = None):
    """Build a started unit whose plan carries the check the info refers to."""
    layer = ops.pebble.Layer({"checks": {name: {"override": "replace"} for name in (info.name,)}})
    container = testing.Container(
        name=CONTAINER, can_connect=True, check_infos={info}, layers={"valkey": layer}
    )
    relation = testing.PeerRelation(
        id=1,
        endpoint=PEER_RELATION,
        local_unit_data={
            "start-state": StartState.STARTED.value,
            "hostname": ENDPOINT,
            **(unit_data or {}),
        },
    )
    status_relation = testing.PeerRelation(id=2, endpoint=STATUS_PEERS_RELATION)
    state = testing.State(
        leader=True, relations={relation, status_relation}, containers={container}
    )
    return container, state


@pytest.mark.parametrize(
    ("check_name", "flag"),
    [
        (VALKEY_HEALTH_CHECK, "valkey-health-check-failed"),
        (SENTINEL_HEALTH_CHECK, "sentinel-health-check-failed"),
    ],
)
def test_check_failed_records_the_unit_as_unhealthy(check_name, flag):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    info = testing.CheckInfo(check_name, failures=3, status=ops.pebble.CheckStatus.DOWN)
    container, state_in = _state_with_check(info)

    state_out = ctx.run(ctx.on.pebble_check_failed(container, info=info), state_in)

    assert state_out.get_relation(1).local_unit_data[flag] == "true"


@pytest.mark.parametrize(
    ("check_name", "flag"),
    [
        (VALKEY_HEALTH_CHECK, "valkey-health-check-failed"),
        (SENTINEL_HEALTH_CHECK, "sentinel-health-check-failed"),
    ],
)
def test_check_recovered_records_the_unit_as_healthy_again(check_name, flag):
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    info = testing.CheckInfo(check_name, failures=0, status=ops.pebble.CheckStatus.UP)
    container, state_in = _state_with_check(info, unit_data={flag: "true"})

    state_out = ctx.run(ctx.on.pebble_check_recovered(container, info=info), state_in)

    assert state_out.get_relation(1).local_unit_data.get(flag) in (None, "false")


def test_a_failed_event_for_a_check_that_recovered_records_it_as_healthy():
    """The handler reads the check's current status, not which event arrived."""
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    info = testing.CheckInfo(VALKEY_HEALTH_CHECK, failures=3, status=ops.pebble.CheckStatus.UP)
    container, state_in = _state_with_check(info, unit_data={"valkey-health-check-failed": "true"})

    state_out = ctx.run(ctx.on.pebble_check_failed(container, info=info), state_in)

    assert state_out.get_relation(1).local_unit_data.get("valkey-health-check-failed") in (
        None,
        "false",
    )


def test_an_unrelated_check_is_ignored():
    ctx = testing.Context(ValkeyCharm, app_trusted=True)
    info = testing.CheckInfo("something-else", failures=3, status=ops.pebble.CheckStatus.DOWN)
    container, state_in = _state_with_check(info)

    state_out = ctx.run(ctx.on.pebble_check_failed(container, info=info), state_in)

    unit_data = state_out.get_relation(1).local_unit_data
    assert "valkey-health-check-failed" not in unit_data
    assert "sentinel-health-check-failed" not in unit_data
