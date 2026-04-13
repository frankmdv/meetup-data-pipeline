import csv
import io
import random
from datetime import datetime, timedelta

import boto3
from faker import Faker

fake = Faker()

S3_BUCKET = "amzn-s3-meetup-bucket-631957124123-us-east-2-an"
S3_PREFIX = "raw/"


def _s3_client():
    return boto3.client("s3")


def _read_csv(s3, key: str, encoding: str = "utf-8"):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    content = obj["Body"].read().decode(encoding, errors="replace")
    reader = csv.reader(io.StringIO(content))
    rows = list(reader)
    return rows[0], rows[1:]


def _read_header(s3, key: str, encoding: str = "utf-8") -> list:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key, Range="bytes=0-4095")
    chunk = obj["Body"].read().decode(encoding, errors="replace")
    first_line = chunk.splitlines()[0]
    return next(csv.reader([first_line]))


def _write_csv(s3, key: str, header: list, rows: list, encoding: str = "utf-8"):
    buffer = io.StringIO()
    writer = csv.writer(buffer, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(header)
    writer.writerows(rows)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=buffer.getvalue().encode(encoding),
        ContentType="text/csv",
    )


def _new_events(group_ids: list, venue_ids: list, n: int = 5) -> list:
    rows = []
    for _ in range(n):
        event_id = f"fake_evt_{random.randint(10_000_000, 99_999_999)}"
        group_id = random.choice(group_ids)
        venue_id = random.choice(venue_ids)
        yes_rsvp = random.randint(5, 150)
        rsvp_limit = yes_rsvp + random.randint(0, 50)
        future_dt = (datetime.now() + timedelta(days=random.randint(1, 30))).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        rows.append(
            [
                event_id,
                now,
                fake.sentence(),
                "7200000",
                f"http://meetup.com/events/{event_id}",
                "cash",
                "0.00",
                "USD",
                "",
                "Price",
                "0",
                now,
                "0",
                "0",
                group_id,
                "open",
                fake.company(),
                fake.slug(),
                "Members",
                "0",
                "Look for the sign",
                str(random.randint(0, 10)),
                f"Taller de {fake.job()}",
                "",
                str(round(random.uniform(3.0, 5.0), 1)),
                str(random.randint(0, 50)),
                str(rsvp_limit),
                "upcoming",
                future_dt,
                now,
                "-18000",
                fake.street_address(),
                "",
                fake.city(),
                "US",
                venue_id,
                str(fake.latitude()),
                "United States",
                str(fake.longitude()),
                fake.company(),
                "",
                "0",
                fake.state_abbr(),
                fake.zipcode(),
                "public",
                "0",
                "Learn and Network",
                str(yes_rsvp),
            ]
        )
    return rows


def _new_members(group_ids: list, n: int = 10) -> list:
    rows = []
    for _ in range(n):
        member_id = random.randint(10_000_000, 99_999_999)
        group_id = random.choice(group_ids)
        joined = (datetime.now() - timedelta(days=random.randint(1, 30))).strftime(
            "%d/%m/%Y, %I:%M:%S p.m."
        )
        visited = datetime.now().strftime("%d/%m/%Y, %I:%M:%S p.m.")

        rows.append(
            [
                str(member_id),
                "",
                fake.city(),
                fake.country_code(),
                "",
                joined,
                str(int(float(fake.latitude()) * 100_000_000)),
                f"http://meetup.com/members/{member_id}",
                str(int(float(fake.longitude()) * 100_000_000)),
                fake.name(),
                fake.state_abbr(),
                "active",
                visited,
                str(group_id),
            ]
        )
    return rows


def generate_traffic(**context) -> None:
    s3 = _s3_client()

    _, groups_rows = _read_csv(s3, f"{S3_PREFIX}groups.csv")
    _, venues_rows = _read_csv(s3, f"{S3_PREFIX}venues.csv")

    group_ids = [r[0] for r in groups_rows if r][:200]
    venue_ids = [r[0] for r in venues_rows if r][:200]

    if not group_ids or not venue_ids:
        print("[generate_traffic] No se encontraron IDs en S3 — omitiendo generación")
        return

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    events_header = _read_header(s3, f"{S3_PREFIX}events.csv")
    new_events = _new_events(group_ids, venue_ids, n=5)
    _write_csv(s3, f"{S3_PREFIX}events_{ts}.csv", events_header, new_events)
    print(f"[generate_traffic] +{len(new_events)} eventos subidos a S3")

    members_header = _read_header(s3, f"{S3_PREFIX}members.csv", encoding="iso-8859-1")
    new_members = _new_members(group_ids, n=10)
    _write_csv(
        s3,
        f"{S3_PREFIX}members_{ts}.csv",
        members_header,
        new_members,
        encoding="iso-8859-1",
    )
    print(f"[generate_traffic] +{len(new_members)} miembros subidos a S3")


if __name__ == "__main__":
    generate_traffic()
