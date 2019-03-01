# Copyright 2009-2013 Yelp, David Marin
# Copyright 2015 Yelp
# Copyright 2017 Yelp
# Copyright 2018 Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Wrappers for gracefully retrying on error."""
import logging
import time

log = logging.getLogger(__name__)


_DEFAULT_BACKOFF = 15
_DEFAULT_MULTIPLIER = 1.5
_DEFAULT_MAX_TRIES = 10
_DEFAULT_MAX_BACKOFF = 1200  # 20 minutes


class RetryWrapper(object):
    """Handle transient errors, with configurable backoff.

    This class can wrap any object. The wrapped object will behave like
    the original one, except that if you call a function and it raises a
    retriable exception, we'll back off for a certain number of seconds
    and call the function again, until it succeeds or we get a non-retriable
    exception.
    """
    def __init__(self, wrapped, retry_if,
                 initial_backoff=_DEFAULT_BACKOFF,
                 multiplier=_DEFAULT_MULTIPLIER,
                 max_tries=_DEFAULT_MAX_TRIES,
                 max_backoff=_DEFAULT_MAX_BACKOFF):
        """
        Wrap the given object

        :param wrapped: the object to wrap
        :param retry_if: a method that takes an exception, and returns whether
                         we should retry
        :type initial_backoff: float
        :param initial_backoff: the number of seconds to wait the first time
                                we get a retriable error
        :type multiplier: float
        :param multiplier: if we retry multiple times, the amount to multiply
                           the backoff time by every time we get an error
        :type max_tries: int
        :param max_tries: how many tries we get. ``0`` means to keep trying
                          forever
        :type max_backoff: float
        :param max_backoff: cap the backoff at this number of seconds
        """
        self.__wrapped = wrapped

        self.__retry_if = retry_if

        self.__initial_backoff = initial_backoff
        if self.__initial_backoff <= 0:
            raise ValueError('initial_backoff must be positive')

        self.__multiplier = multiplier
        if self.__multiplier < 1:
            raise ValueError('multiplier must be at least one!')

        self.__max_tries = max_tries

        self.__max_backoff = max_backoff

    def __iter__(self):
        """The paginator uses `_make_request` to make any API calls, so we
        wrap this method manually in our retry logic and pass the iteration
        back to the paginator."""
        if hasattr(self.__wrapped, '__iter__'):
            self.__wrapped._make_request = \
                self.__wrap_method_with_call_and_maybe_retry(
                    self.__wrapped._make_request)
            return self.__wrapped.__iter__()
        raise Exception('Not iterable')

    def __getattr__(self, name):
        """The glue that makes functions retriable, and returns other
        attributes from the wrapped object as-is."""
        x = getattr(self.__wrapped, name)
        if hasattr(x, '__call__'):
            return self.__wrap_method_with_call_and_maybe_retry(x)
        else:
            return x

    def __wrap_method_with_call_and_maybe_retry(self, f):
        """Wrap method f in a retry loop."""

        def call_and_maybe_retry(*args, **kwargs):
            backoff = self.__initial_backoff
            tries = 0

            while not self.__max_tries or tries < self.__max_tries:
                try:
                    return f(*args, **kwargs)
                except Exception as ex:
                    if (self.__retry_if(ex) and
                        (tries < self.__max_tries - 1 or
                         not self.__max_tries)):
                        log.info('Got retriable error: %r' % ex)
                        log.info('Backing off for %.1f seconds' % backoff)
                        time.sleep(backoff)
                        tries += 1
                        backoff *= self.__multiplier
                        backoff = min(backoff, self.__max_backoff)
                    else:
                        raise

        # pretend to be the original function
        call_and_maybe_retry.__doc__ = f.__doc__

        if hasattr(f, '__name__'):
            call_and_maybe_retry.__name__ = f.__name__

        return call_and_maybe_retry
