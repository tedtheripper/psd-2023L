from dataclasses import dataclass
from datetime import datetime
from typing import Dict
from kafka import KafkaProducer
import numpy as np


@dataclass
class InvestmentParameters:
    name: str
    mu: float
    sigma_sq: float


@dataclass
class RateOfReturn:
    investment_name: str
    value: float
    timestamp: int


def rate_to_dict(rate: RateOfReturn) -> Dict[str, any]:
    return {
        "investmentName": rate.investment_name,
        "rate": rate.value,
        "timestamp": rate.timestamp,
    }


def generate_rate_of_return(investment_params: InvestmentParameters) -> RateOfReturn:
    mu = investment_params.mu
    sigma = np.sqrt(investment_params.sigma_sq)

    rate_value = np.random.normal(mu, sigma)
    
    while abs(rate_value) > 0.1:
        rate_value = np.random.normal(mu, sigma)
    
    curr_timestamp = datetime.timestamp(datetime.now())
    return RateOfReturn(investment_params.name, rate_value, curr_timestamp)


def send_to_kafka(producer: KafkaProducer, rate: RateOfReturn) -> None:
    producer.send(rate.investment_name, rate_to_dict(rate))
