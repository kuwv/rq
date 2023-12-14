import ctypes
import signal
import threading
from types import FrameType, TracebackType
from typing import Any, Optional, Type


class BaseTimeoutException(Exception):
    """Base exception for timeouts."""

    pass


class JobTimeoutException(BaseTimeoutException):
    """Raised when a job takes longer to complete than the allowed maximum
    timeout value.
    """

    pass


class HorseMonitorTimeoutException(BaseTimeoutException):
    """Raised when waiting for a horse exiting takes longer than the maximum
    timeout value.
    """

    pass


class BaseDeathPenalty:
    """Base class to setup job timeouts."""

    def __init__(
        self, timeout: int, exception: Type[BaseTimeoutException] = BaseTimeoutException, **kwargs: Any
    ) -> None:
        self._timeout = timeout
        self._exception = exception

    def __enter__(self) -> None:
        self.setup_death_penalty()

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: BaseException,
        exc_tb: Optional[TracebackType],
    ) -> None:
        # Always cancel immediately, since we're done
        try:
            self.cancel_death_penalty()
        except BaseTimeoutException:
            # Weird case: we're done with the with body, but now the alarm is
            # fired.  We may safely ignore this situation and consider the
            # body done.
            pass

    def setup_death_penalty(self) -> None:
        raise NotImplementedError()

    def cancel_death_penalty(self) -> None:
        raise NotImplementedError()


class UnixSignalDeathPenalty(BaseDeathPenalty):
    def handle_death_penalty(self, signum: int, frame: Optional[FrameType] = None) -> None:
        raise self._exception('Task exceeded maximum timeout value ({0} seconds)'.format(self._timeout))

    def setup_death_penalty(self) -> None:
        """Sets up an alarm signal and a signal handler that raises
        an exception after the timeout amount (expressed in seconds).
        """
        signal.signal(signal.SIGALRM, self.handle_death_penalty)
        signal.alarm(self._timeout)

    def cancel_death_penalty(self) -> None:
        """Removes the death penalty alarm and puts back the system into
        default signal handling.
        """
        signal.alarm(0)
        signal.signal(signal.SIGALRM, signal.SIG_DFL)


class TimerDeathPenalty(BaseDeathPenalty):
    def __init__(self, timeout: int, exception: Type[BaseException] = JobTimeoutException, **kwargs: Any) -> None:
        super().__init__(timeout, exception, **kwargs)
        self._target_thread_id = threading.current_thread().ident
        self._timer: Optional[threading.Timer] = None

        # Monkey-patch exception with the message ahead of time
        # since PyThreadState_SetAsyncExc can only take a class
        def init_with_message(self, *args: Any, **kwargs: Any) -> None:  # type: ignore # noqa
            super(Exception, self).__init__(  # type: ignore
                "Task exceeded maximum timeout value ({0} seconds)".format(timeout)
            )

        setattr(self._exception, '__init__', init_with_message)

    def new_timer(self) -> threading.Timer:
        """Returns a new timer since timers can only be used once."""
        return threading.Timer(self._timeout, self.handle_death_penalty)

    def handle_death_penalty(self) -> None:
        """Raises an asynchronous exception in another thread.

        Reference http://docs.python.org/c-api/init.html#PyThreadState_SetAsyncExc for more info.
        """
        ret = ctypes.pythonapi.PyThreadState_SetAsyncExc(
            ctypes.c_long(self._target_thread_id), ctypes.py_object(self._exception)
        )
        if ret == 0:
            raise ValueError("Invalid thread ID {}".format(self._target_thread_id))
        elif ret > 1:
            ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(self._target_thread_id), 0)
            raise SystemError("PyThreadState_SetAsyncExc failed")

    def setup_death_penalty(self) -> None:
        """Starts the timer."""
        if self._timeout <= 0:
            return
        self._timer = self.new_timer()
        self._timer.start()

    def cancel_death_penalty(self) -> None:
        """Cancels the timer."""
        if self._timeout <= 0:
            return
        self._timer.cancel()
        self._timer = None
