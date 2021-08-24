from marshmallow.fields import Integer, String, List, DateTime, Float
from marshmallow import Schema, validate

from validators.validate_fields import Interval


class CreateCouriersSchema(Schema):
    courier_id = Integer(required=True)
    courier_type = String(required=True)
    regions = List(Integer(), required=True)
    working_hours = List(Interval(), required=True)


class PatchCourierSchema(Schema):
    courier_id = Integer(required=True)
    courier_type = String()
    regions = List(Integer())
    working_hours = List(Interval())


class GetCourierInfoSchema(Schema):
    courier_id = Integer(required=True)


class CreateOrdersSchema(Schema):
    order_id = Integer(required=True)
    weight = Float(required=True, validate=validate.Range(min=0.01, max=50))
    region = Integer(required=True)
    delivery_hours = List(Interval(), required=True)


class AssignOrderSchema(Schema):
    courier_id = Integer(required=True)


class CompleteOrderSchema(Schema):
    courier_id = Integer(required=True)
    order_id = Integer(required=True)
    complete_time = DateTime(required=True)
