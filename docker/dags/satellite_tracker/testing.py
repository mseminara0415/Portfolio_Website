from datetime import datetime

timestamp_value = datetime.now()
timestamp_full = timestamp_value.strftime("%m-%d-%Y-%H-%M-%S")
timestamp_month = timestamp_value.strftime("%M-%Y")
print(f"timestamp now value is {timestamp_value}")
print(f"timestamp month-year is {timestamp_month}")