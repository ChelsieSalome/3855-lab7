from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from models import Base, PerformanceReading, ErrorReading

engine = create_engine('sqlite:///monitoring.db', echo=True)
Base.metadata.create_all(engine)
SessionLocal = sessionmaker(bind=engine)


print()
print("TEST 1: Insert Performance Reading")
print()

session = SessionLocal()
try:
    test_reading = PerformanceReading(
        avg_cpu=71.83,
        avg_memory=64.07,
        avg_disk_io=121.67,
        num_readings=3
    )
    session.add(test_reading)
    session.commit()
    print(f"Performance data added to the DB")
except Exception as e:
    session.rollback()
    print(f"Error: {e}")
finally:
    session.close()

print()

print()
print("TEST 2: Insert Error Reading")
print()

session = SessionLocal()
try:
    test_error = ErrorReading(
        client_errors_4xx=5,
        server_errors_5xx=3,
        avg_severity_level=3.5,
        avg_response_time=1200.75,
        num_errors=8
    )
    session.add(test_error)
    session.commit()
    print(f"Error data added to the DB")

except Exception as e:
    session.rollback()
    print(f"Error: {e}")
finally:
    session.close()

# print()

# print()
# print("TEST 3: Query All Performance Readings")
# print()

# session = SessionLocal()
# try:
#     stmt = select(PerformanceReading)
#     results = session.execute(stmt).scalars().all()
#     print(f"Found {len(results)} performance reading(s):\n")
#     for reading in results:
#         print(f"  ID: {reading.id}")
#         print(f"  Avg CPU: {reading.avg_cpu}%")
#         print(f"  Avg Memory: {reading.avg_memory}%")
#         print(f"  Avg Disk I/O: {reading.avg_disk_io}")
#         print(f"  Num Readings: {reading.num_readings}")
#         print(f"  Created: {reading.date_created}")
#         print()
# finally:
#     session.close()

# print()

# print("=" * 60)
# print("TEST 4: Query All Error Readings")
# print("=" * 60)

# session = SessionLocal()
# try:
#     stmt = select(ErrorReading)
#     errors = session.execute(stmt).scalars().all()
#     print(f"Found {len(errors)} error reading(s):\n")
#     for error in errors:
#         print(f"  ID: {error.id}")
#         print(f"  Client Errors (4xx): {error.client_errors_4xx}")
#         print(f"  Server Errors (5xx): {error.server_errors_5xx}")
#         print(f"  Avg Severity: {error.avg_severity_level}")
#         print(f"  Avg Response Time: {error.avg_response_time}ms")
#         print(f"  Num Errors: {error.num_errors}")
#         print(f"  Created: {error.date_created}")
#         print()
# finally:
#     session.close()

# print("All tests completed!")
