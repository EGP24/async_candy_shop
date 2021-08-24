from marshmallow import fields, ValidationError
from datetime import datetime


class Interval(fields.Field):
    def validate_interval(self, interval):
        try:
            time_start = datetime.strptime(interval.split('-')[0], '%H:%M').time()
            time_end = datetime.strptime(interval.split('-')[1], '%H:%M').time()
            return [time_start, time_end]
        except:
            raise ValidationError(f'"{interval}" cannot be formatted as a interval.')

    def _serialize(self, value, attr, obj, **kwargs):
        return self.validate_interval(value)

    def _deserialize(self, value, attr, data, **kwargs):
        return self.validate_interval(value)