from collections import defaultdict
from utils import InvestmentParameters, generate_rate_of_return
# from producer import investments

investments = [
    InvestmentParameters("A", 1e-3, 1),
    InvestmentParameters("B", 4e-3, 16),
    InvestmentParameters("C", 2e-3, 4),
    InvestmentParameters("D", 4e-3, 25),
    InvestmentParameters("E", 3e-3, 9),
]

rates = {
    "A": [],
    "B": [],
    "C": [],
    "D": [],
    "E": [],
}

for j, inv_name in enumerate(rates.keys()):
    for i in range(int(1e6)):
        rate = generate_rate_of_return(investments[j])
        rates[inv_name].append(rate.value)

    values = rates[inv_name]

    avg = sum(values) / len(values)
    sorted_numbers = sorted(rates[inv_name])

    index = int(0.1 * len(sorted_numbers))

    quantile = sorted_numbers[index]

    average_of_smallest_10_percent = (
        sum(sorted_numbers[:index]) / index if index > 0 else 0
    )
    
    print(f"{inv_name} | avg: {avg} | q: {quantile} | avgSmall: {average_of_smallest_10_percent}")
