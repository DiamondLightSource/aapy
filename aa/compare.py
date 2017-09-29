import logging as log


DIFF_EVENT = 'Different events with identical timestamps:\n{}\n{}'


class ArchiveDataDiff(object):

    LEFT = 'left'
    RIGHT = 'right'

    def __init__(self):
        self._extra = {self.LEFT: [], self.RIGHT: []}
        self._duplicates = {self.LEFT: [], self.RIGHT: []}
        self._invalid = {self.LEFT: [], self.RIGHT: []}

    def __eq__(self, other):
        return (self._extra == other._extra and
                self._duplicates == other._duplicates and
                self._invalid == other._invalid)

    def add_extra(self, side, event):
        self._extra[side].append(event)

    def add_duplicate(self, side, event):
        self._duplicates[side].append(event)

    def add_invalid(self, side, event):
        self._invalid[side].append(event)

    def summarise(self):
        for side in (self.LEFT, self.RIGHT):
            for event in self._extra[side]:
                log.warn('Extra event on {}: {}'.format(side, event))
            for event in self._duplicates[side]:
                log.info('Duplicate event on {}: {}'.format(side, event))
            for event in self._invalid[side]:
                log.info('High severity event on {}: {}'.format(side, event))


def compare_archive_data(data_left, data_right):
    log.info('Comparing left {} right {}'.format(data_left.values.shape, data_right.values.shape))
    diff = ArchiveDataDiff()
    previous_left_event = None
    previous_right_event = None
    count_left = 0
    count_right = 0
    while not (count_left >= len(data_left.values) and
                       count_right >= len(data_right.values)):
        if not count_right >= len(data_right.values):
            right_event = data_right.get_event(count_right)
            if right_event == previous_right_event:
                diff.add_duplicate(ArchiveDataDiff.RIGHT, right_event)
                count_right += 1
                continue
            elif right_event.severity > 3:
                diff.add_invalid(ArchiveDataDiff.RIGHT, right_event)
                count_right += 1
                continue
            if count_left >= len(data_left.values):
                diff.add_extra(ArchiveDataDiff.RIGHT, right_event)
                count_right += 1
                continue
        if not count_left >= len(data_left.values):
            left_event = data_left.get_event(count_left)
            if left_event == previous_left_event:
                diff.add_duplicate(ArchiveDataDiff.LEFT, left_event)
                count_left += 1
                continue
            elif left_event.severity > 3:
                diff.add_invalid(ArchiveDataDiff.LEFT, left_event)
                count_left += 1
                continue
            if count_right >= len(data_right.values):
                diff.add_extra(ArchiveDataDiff.LEFT, left_event)
                count_left += 1
                continue

        log.debug('Count_left %d count_right %d', count_left, count_right)
        log.debug('Left event %s', format(left_event))
        log.debug('Right event %s', format(right_event))

        if left_event.timestamp > right_event.timestamp:
            diff.add_extra(ArchiveDataDiff.RIGHT, right_event)
            count_right += 1
            previous_right_event = right_event
        elif right_event.timestamp > left_event.timestamp:
            diff.add_extra(ArchiveDataDiff.LEFT, left_event)
            count_left += 1
            previous_left_event = left_event
        else:
            assert left_event == right_event, DIFF_EVENT.format(left_event,
                                                                right_event)
            count_left += 1
            count_right += 1
            previous_left_event = left_event
            previous_right_event = right_event

    return diff

