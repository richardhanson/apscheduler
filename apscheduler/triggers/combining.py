from apscheduler.triggers.base import BaseTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.util import obj_to_ref, ref_to_obj
from functools import reduce
from datetime import datetime, timedelta
import math


def lcm(x: int, y: int) -> int:
    return int(x * y / math.gcd(x, y))


class BaseCombiningTrigger(BaseTrigger):
    __slots__ = ('triggers', 'jitter')

    def __init__(self, triggers, jitter=None):
        self.triggers = triggers
        self.jitter = jitter

    def __getstate__(self):
        return {
            'version': 1,
            'triggers': [(obj_to_ref(trigger.__class__), trigger.__getstate__())
                         for trigger in self.triggers],
            'jitter': self.jitter
        }

    def __setstate__(self, state):
        if state.get('version', 1) > 1:
            raise ValueError(
                'Got serialized data for version %s of %s, but only versions up to 1 can be '
                'handled' % (state['version'], self.__class__.__name__))

        self.jitter = state['jitter']
        self.triggers = []
        for clsref, state in state['triggers']:
            cls = ref_to_obj(clsref)
            trigger = cls.__new__(cls)
            trigger.__setstate__(state)
            self.triggers.append(trigger)

    def __repr__(self):
        return '<{}({}{})>'.format(self.__class__.__name__, self.triggers,
                                   ', jitter={}'.format(self.jitter) if self.jitter else '')


class AndTrigger(BaseCombiningTrigger):
    """
    Always returns the earliest next fire time that all the given triggers can agree on.
    The trigger is considered to be finished when any of the given triggers has finished its
    schedule.

    Trigger alias: ``and``

    :param list triggers: triggers to combine
    :param int|None jitter: advance or delay the job execution by ``jitter`` seconds at most.
    """

    __slots__ = ()

    def _extract_time_length(self, previous_fire_time, now):
        ret = []
        for trigger in self.triggers:
            if isinstance(trigger, IntervalTrigger):
                ret.append(
                    (int(trigger.interval_length), trigger.start_date, trigger.end_date)
                )
            else:
                ret.append(
                    (trigger.get_next_fire_time(previous_fire_time, now), trigger.start_date,
                     trigger.end_date)
                )
            # elif isinstance(trigger, CronTrigger):
            #     cron_next = trigger.get_next_fire_time(previous_fire_time, now)
            #     ret.append((int((now - cron_next).total_seconds()), trigger.start_date,
            #                 trigger.end_date))
            # elif isinstance(trigger, DateTrigger):
            #     ret.append((trigger.run_date - previous_fire_time, None, None))
        return ret

    def get_next_fire_time(self, previous_fire_time, now):
        # TODO so this doesn't work because it doesn't know about start/end times of triggers...
        if not self.triggers:
            return
        if len(self.triggers) == 1:
            return self.triggers[0].get_next_fire_time(previous_fire_time, now)

        # lengths = self._extract_time_length(previous_fire_time, now)
        lengths = map(lambda x: x.interval_length if isinstance(x, IntervalTrigger), self.triggers)
        # dates = list(filter(lambda x: isinstance(x, datetime), lengths))
        # not_dates = filter(lambda x: isinstance(x, int), lengths)
        lcm_secs = reduce(lcm, lengths)
        dates = list(filter(lambda x: not isinstance(x, IntervalTrigger), self.triggers))

        for d in dates:
            if not (d.get_next_fire_time(previous_fire_time, now) - now).total_seconds() / lcm_secs:
                return None
        if dates:
            while True:
                fire_times = [trigger.get_next_fire_time(previous_fire_time, now)
                              for trigger in self.triggers if isinstance(trigger, DateTrigger)]
                if None in fire_times:
                    return None
                elif min(fire_times) == max(fire_times):
                    return self._apply_jitter(fire_times[0], self.jitter, now)
                else:
                    now = max(fire_times)
        elif previous_fire_time is not None:
            return previous_fire_time + timedelta(seconds=lcm_secs)
        else:
            return now + timedelta(seconds=lcm_secs)

    def __str__(self):
        return 'and[{}]'.format(', '.join(str(trigger) for trigger in self.triggers))


class OrTrigger(BaseCombiningTrigger):
    """
    Always returns the earliest next fire time produced by any of the given triggers.
    The trigger is considered finished when all the given triggers have finished their schedules.

    Trigger alias: ``or``

    :param list triggers: triggers to combine
    :param int|None jitter: advance or delay the job execution by ``jitter`` seconds at most.

    .. note:: Triggers that depends on the previous fire time, such as the interval trigger, may
        seem to behave strangely since they are always passed the previous fire time produced by
        any of the given triggers.
    """

    __slots__ = ()

    def get_next_fire_time(self, previous_fire_time, now):
        fire_times = [trigger.get_next_fire_time(previous_fire_time, now)
                      for trigger in self.triggers]
        fire_times = [fire_time for fire_time in fire_times if fire_time is not None]
        if fire_times:
            return self._apply_jitter(min(fire_times), self.jitter, now)
        else:
            return None

    def __str__(self):
        return 'or[{}]'.format(', '.join(str(trigger) for trigger in self.triggers))
