#!/usr/bin/env python3
import click
import csv
from datetime import datetime
from decimal import Decimal
import os
import sqlite3
from typing import Dict
from balances_calculator import BalancesCalculator


@click.group()
@click.option("--debug/--no-debug", default=False, help="Debug output, or no debug output.")
@click.pass_context
def interface(ctx: Dict, debug: bool) -> None:
    """Ampla engineering takehome ledger calculator."""
    ctx.ensure_object(dict)
    ctx.obj["DEBUG"] = debug  # you can use ctx.obj['DEBUG'] in other commands to log or print if DEBUG is on
    ctx.obj["DB_PATH"] = os.path.join(os.getcwd(), "db.sqlite3")
    if debug:
        click.echo(f"[Debug mode is on]")


@interface.command()
@click.pass_context
def create_db(ctx: Dict) -> None:
    """Initialize sqlite3 database."""
    if os.path.exists(ctx.obj["DB_PATH"]):
        click.echo("Database already exists")
        return

    with sqlite3.connect(ctx.obj["DB_PATH"]) as connection:
        if not connection:
            click.echo(
                "Error: Unable to create sqlite3 db file. Please ensure sqlite3 is installed on your system and "
                "available in PATH!"
            )
            return

        cursor = connection.cursor()
        cursor.execute(
            """
            create table events
            (
                id integer not null primary key autoincrement,
                type varchar(32) not null,
                amount decimal not null,
                date_created date not null
                CHECK (type IN ("advance", "payment"))
            );
        """
        )
        connection.commit()
    click.echo(f"Initialized database at {ctx.obj['DB_PATH']}")


@interface.command()
@click.pass_context
def drop_db(ctx: Dict) -> None:
    """Delete sqlite3 database."""
    if not os.path.exists(ctx.obj["DB_PATH"]):
        click.echo(f"SQLite database does not exist at {ctx.obj['DB_PATH']}")
    else:
        os.unlink(ctx.obj["DB_PATH"])
        click.echo(f"Deleted SQLite database at {ctx.obj['DB_PATH']}")


@interface.command()
@click.argument("filename", type=click.Path(exists=True, writable=False, readable=True))
@click.pass_context
def load(ctx: Dict, filename: str) -> None:
    """Load events with data from csv file."""
    if not os.path.exists(ctx.obj["DB_PATH"]):
        click.echo(f"Database does not exist at {ctx.obj['DB_PATH']}, please create it using `create-db` command")
        return

    loaded = 0
    with open(filename) as infile, sqlite3.connect(ctx.obj["DB_PATH"]) as connection:
        cursor = connection.cursor()
        reader = csv.reader(infile)
        for row in reader:
            cursor.execute(
                f"insert into events (type, amount, date_created) values (?, ?, ?)", (row[0], row[2], row[1])
            )
            loaded += 1
        connection.commit()

    click.echo(f"Loaded {loaded} events from {filename}")


@interface.command()
@click.argument("end_date", required=False, type=click.STRING)
@click.pass_context
def balances(ctx: Dict, end_date: str = None) -> None:
    """Display balance statistics as of `end_date`."""
    # NOTE: You may not change the function signature of `balances`,
    #       however you may implement it any way you want, so long
    #       as you adhere to the format specification.
    #       Here is some code to get you started!
    if end_date is None:
        end_date = datetime.now().date().isoformat()

    # defines the end_date as datetime.date object
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

    # inits the balances calculator
    bc = BalancesCalculator(end_date=end_date)

    # query events from database example
    with sqlite3.connect(ctx.obj["DB_PATH"]) as connection:
        cursor = connection.cursor()
        result = cursor.execute(f"""
            select * from events 
            where date_created <= ? 
            order by date_created asc;""", (end_date,))
        events = result.fetchall()

        # loop and process events
        for event in events:

            event_type = event[1]
            event_amount = Decimal(event[2])
            event_date = datetime.strptime(event[3], '%Y-%m-%d').date()

            bc.process_event(event_date=event_date, event_type=event_type, amount=event_amount)

    # finish the balances calculation and get results
    results = bc.finish_calculation_and_get_results()

    click.echo("Advances:")
    click.echo("----------------------------------------------------------")
    # NOTE: This initial print adheres to the format spec.
    click.echo("{0:>10}{1:>11}{2:>17}{3:>20}".format("Identifier", "Date", "Initial Amt", "Current Balance"))

    for i, advance in enumerate(results.advances):
        click.echo("{0:>10}{1:>11}{2:>17.2f}{3:>20.2f}".format(
            i + 1, advance.event_date.strftime("%Y-%m-%d"), advance.original_amount, advance.current_balance
        ))

    overall_advance_balance = results.overall_advance_balance
    overall_interest_payable_balance = results.overall_interest_payable_balance
    overall_interest_paid = results.overall_interest_paid
    overall_payments_for_future = results.overall_payments_for_future

    # print summary statistics
    # NOTE: These prints adhere to the format spec.
    click.echo("\nSummary Statistics:")
    click.echo("----------------------------------------------------------")
    click.echo("Aggregate Advance Balance: {0:31.2f}".format(overall_advance_balance))
    click.echo("Interest Payable Balance: {0:32.2f}".format(overall_interest_payable_balance))
    click.echo("Total Interest Paid: {0:37.2f}".format(overall_interest_paid))
    click.echo("Balance Applicable to Future Advances: {0:>19.2f}".format(overall_payments_for_future))


if __name__ == "__main__":
    interface()
