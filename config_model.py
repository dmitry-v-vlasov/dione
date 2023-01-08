from pydantic import BaseModel

import typing

from datetime import datetime


class TimeRange(BaseModel):
    begin_time: datetime
    end_time: datetime


class LocalDataLoading(BaseModel):
    file_name: str


class RemoteDataLoading(BaseModel):
    source_name: str
    file_name: str
    time_range: TimeRange


class DataLoading(BaseModel):
    data_loading_stategy: str
    remote_data_loading: RemoteDataLoading
    local_data_loading: LocalDataLoading
    date_column: str


class DataTransformation(BaseModel):
    tending: typing.OrderedDict
    clearing: typing.OrderedDict
    treatment: typing.OrderedDict
    scaling: typing.OrderedDict


class QuotedInstrument(BaseModel):
    ticker: str
    name: str
    description: str
    data_loading: DataLoading
    data_transformation: DataTransformation


class MachineLearning(BaseModel):
    time_range: TimeRange
    split_time: datetime
    cross_validation_strategy: str


class Research(BaseModel):
    name: str
    description: str
    machine_learning: MachineLearning
    target_quoted_instrument: QuotedInstrument
    quoted_instruments: typing.List[QuotedInstrument]


class Config(BaseModel):
    """
    Configuration for the project Dione.
    """

    research: Research
