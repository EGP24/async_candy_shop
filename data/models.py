from sqlalchemy import Column, Integer, String, ForeignKey, Time, DateTime, Float, Boolean
from sqlalchemy.orm import declarative_base, relation

Base = declarative_base()


class CourierType(Base):
    __tablename__ = 'courier_types'

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String, unique=True, nullable=False)
    carrying = Column(Integer, nullable=False)
    coefficient = Column(Integer, nullable=False)

    couriers = relation('Courier', back_populates='type')


class Courier(Base):
    __tablename__ = 'couriers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    type_id = Column(Integer, ForeignKey('courier_types.id'), nullable=False)
    time_last_complete_order = Column(DateTime, nullable=True)
    earning = Column(Integer, default=0, nullable=False)

    type = relation('CourierType')

    orders = relation('Order', back_populates='courier')
    regions = relation('Region', back_populates='courier')
    courier_intervals = relation('CourierInterval', back_populates='courier')

    @property
    def rating(self):
        regions = list(filter(lambda region: region.orders_count != 0, self.regions))
        min_delivery_time = min([60 * 60] + [region.sum_time / region.orders_count for region in regions])
        rating = (60 * 60 - min_delivery_time) / 60 / 60 * 5
        return round(rating, 2)

    def to_dict(self, full_info=False):
        courier_data = {'courier_id': self.id, 'courier_type': self.type.title,
                        'regions': [region.number_region for region in self.regions],
                        'working_hours': [str(interval) for interval in self.courier_intervals]}
        if full_info:
            courier_data['earnings'] = self.earning
            courier_data['rating'] = self.rating
        return courier_data


class Order(Base):
    __tablename__ = 'orders'

    id = Column(Integer, primary_key=True, autoincrement=True)
    weight = Column(Float, nullable=False)
    region_number = Column(Integer, nullable=False)
    is_complete = Column(Boolean, default=False, nullable=False)
    is_assign = Column(Boolean, default=False, nullable=False)
    courier_id = Column(Integer, ForeignKey("couriers.id"), nullable=True)
    assign_time = Column(DateTime, nullable=True)

    courier = relation('Courier')

    delivery_hours = relation('OrderInterval', back_populates='order')


class Region(Base):
    __tablename__ = 'regions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    number_region = Column(Integer, nullable=False)
    courier_id = Column(Integer, ForeignKey("couriers.id"), nullable=False)
    orders_count = Column(Integer, default=0, nullable=False)
    sum_time = Column(Integer, default=0, nullable=False)

    courier = relation('Courier')


class CourierInterval(Base):
    __tablename__ = 'courier_intervals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    courier_id = Column(Integer, ForeignKey('couriers.id'), nullable=False)
    time_start = Column(Time, nullable=False)
    time_end = Column(Time, nullable=False)

    courier = relation('Courier')

    def __str__(self):
        return f'{str(self.time_start)[:-3]}-{str(self.time_end)[:-3]}'


class OrderInterval(Base):
    __tablename__ = 'order_intervals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    order_id = Column(Integer, ForeignKey('orders.id'), nullable=False)
    time_start = Column(Time, nullable=False)
    time_end = Column(Time, nullable=False)

    order = relation('Order')

    def __str__(self):
        return f'{str(self.time_start)[:-3]}-{str(self.time_end)[:-3]}'