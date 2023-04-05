# Copyright 2021 Red Hat, Inc
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module containing unit tests for the sentry utilities module."""

import logging
import os

from ccx_data_pipeline.utils.sentry import get_event_level
from ccx_data_pipeline.utils.sentry import init_sentry


def test_utils_sentry_get_event_level():
    """Verify sentry capture level depending on SENTRY_CATCH_WARNINGS."""
    if "SENTRY_CATCH_WARNINGS" in os.environ:
        del os.environ["SENTRY_CATCH_WARNINGS"]
    assert get_event_level() == logging.ERROR

    os.environ["SENTRY_CATCH_WARNINGS"] = "1"
    assert get_event_level() == logging.WARNING


def test_utils_sentry_init_sentry_bad_dsn():
    """Verify sentry is not started if bad dsn provided and error is reported."""
    err_message = "test error"
    events = []
    init_sentry("http://test@localhost", events.append, None)

    logging.error(err_message)

    assert len(events) == 1
    assert events[0]["level"] == "error"
    assert events[0]["logentry"]["message"] == err_message


def test_utils_sentry_init_sentry_no_dsn():
    """Verify sentry is not started if no dsn provided."""
    events = []
    init_sentry("http://test@localhost", events.append, None)

    assert len(events) == 0
