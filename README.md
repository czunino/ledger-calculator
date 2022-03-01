# Ampla Engineering Take-home Exercise

## Loan Ledger

As part of your new role at Working Capital you are asked to continue the work that another engineer has started for
the creation of a ledger calculator for a revolving line of credit. She left some documentation that you can find at the
bottom of this document. Please read this spec in its entirety to ensure no details are missed.

Working Capital offers *advances* (loans) to business customers at a fixed interest rate but without a fixed payment
schedule. Each advance is like a mini-loan with its own running ledgers of "*advance balance*" (the amount of the
advance that has yet to be repaid). Advances are paid off by the customer by means of *payments*. An advance doesn't
need to be paid off before a new advance is requested, so a customer can have multiple active advances (advances that
are not yet paid off) at the same time.

## Events CSV file format

Loading of loan events (advances and repayments), can be simplified and standardized using a csv file in the following
format: `TYPE,DATE,AMOUNT`

```
advance,2021-05-22,2250.00
payment,2021-06-03,250.00
advance,2021-07-05,1200.00
payment,2021-07-28,4000.00
advance,2021-10-01,100.00
```

All events (advances and payments) in the CSV file are in sequential order.

## Boilerplate Command Line Interface

The ledger system has a CLI which supports the following commands: 

- a `create-db` command to initialize the sqlite database
- a `drop-db` command to delete the database
- a `load` command to load a csv file that contains advance and payment events

### Your Task
Your task for this project is to implement the `balances` command.
You may add as many additional methods and data structures as you want,
but make sure the solution is in the placeholder text marked `FIXME`
in `cli.py` (in other words, that except for the placeholder, existing
code does not change).

The `balances` command should:
 - Optionally take in `end_date` (YYYY-MM-DD) as command argument for calculations


 - Output a table containing the following information for each and every advance (all as of `end_date` if supplied, today's date otherwise):
   - the advance index identifier
   - creation date
   - original advance amount
   - current advance balance: how much still needs to be repaid


 - Output Summary statistics (all as of `end_date` if supplied, today's date otherwise):
   - the sum of advance balances across all advances
   - the total *interest payable balance*
   - the total of *interest paid*
   - the total amount of accrued payments applicable to future advances

##### *Additional Requirements / Notes*

- Your output for `balances` needs to match the format below exactly as shown (see examples below).
    - **Format Specification**:
        - `Identifier` should be right justified with 10 spaces.
        - `Date` should be right justified with 11 spaces after `Identifier`.
        - `Initial Amt` should be right justified with 17 spaces after `Date` rounded to the nearest hundredths place.
        - `Current Balance` should be right justified with 20 spaces after `Initial Amt` and rounded to the nearest hundredths place.
    - See `click.echo()` examples in the starter code on how to right justify your output, and to round to two decimal places.
- Your solution may not change the function signature of `balances`.
- Your output will be compared against a suite of correct output files, see `tests/test*.correct.txt`
- You do not need to support changes to events after they have been loaded.
- You may not use any libraries related to loans, spreadsheets, or accounting.
- Your solution should support
    - large number of events (1M+)
    - fast response for the balances even when there is a large number of events in the database

#### Interest Calculations

Each customer has an "interest payable balance" (the amount of interest that has accrued and has yet to be paid). The interest accrued each day is based on the following formula:

"*daily accrued interest"* = 0.00035 x "*the sum of all advance balances*"

For example, if there are two open advances:

- one advance on `2021-01-01` for $100,000
- another advance on `2021-01-05` for $200,000

The total accrued interest calculation as of `2021-01-08` would look like the following:

```
      Date    Sum Adv Balances   Daily Accrued Interest  Interest Payable Balance
2021-01-01          100,000.00                    35.00                     35.00
2021-01-02          100,000.00                    35.00                     70.00
2021-01-03          100,000.00                    35.00                    105.00
2021-01-04          100,000.00                    35.00                    140.00
2021-01-05          300,000.00                   105.00                    245.00
2021-01-06          300,000.00                   105.00                    350.00
2021-01-07          300,000.00                   105.00                    455.00
2021-01-08          300,000.00                   105.00                    560.00
```

So on `2021-01-08`, the interest payable balance would be $560.00, this table is only used to show the interest calculation on a daily basis.

#### Payment Calculations

Anytime a payment is received, it is applied in the following manner:

1. First, to reduce the "*interest payable balance*" for the customer (talked about above), if any,
2. Second, any remaining amount of the repayment is applied to reduce the "advance balance" of the *oldest* active
   advance, and if there is any remaining amount it reduces the amount of the following (second oldest) advance, and so
   on,
3. Finally - after *all* advances have been repaid - if there is still some amount of the repayment available, the remaining
   amount of the repayment should be credited towards to immediately paying down future advances, when they are made.

## Setup Instructions
Below is the code's current documentation. Please feel free to update and extend it when submitting your codebase!

### Running the code

#### Requirements & Installation

The only system requirement is `sqlite3`, all other python requirements will be installed via `pip`. The only python
requirements are `click` and it's dependencies.

#### Python Environment Installation

1. Clone the repo: `git clone git@github.com:Gourmet-Growth/ampla-ledger-takehome.git` and make a new virtual
   environment within the repo using: `cd ampla-ledger-takehome; python3 -m venv env`
2. Activate the environment: `source env/bin/activate`
3. Upgrade pip and install requirements.txt: `pip install -U pip; pip install -r requirements.txt`

#### Executing the CLI
Run `python cli.py` for a list of commands and helpful output.

#### Testing the CLI

Run `python -m unittest tests.test_cli` for testing the command line. All test cases should pass if balances is
implemented correctly. NOTE: With the given starter code you will pass 3 out of 16 tests out of the box. Please use
the test suite to gauge your precision and accuracy. There are 13 tests for `balances`.

## Example

Assume the following CSV of advances and payments (which you can find in `tests/test2.csv`):

```
advance,2021-05-22,2250.00
payment,2021-06-03,250.00
advance,2021-07-05,1200.00
payment,2021-07-28,4000.00
advance,2021-08-04,1500.00
```

Find below the expected outputs after loading the events in a fresh database and running the `balances`
command for the given dates:
```
$ python cli.py balances 2021-07-20
Advances:
----------------------------------------------------------
Identifier       Date      Initial Amt     Current Balance
         1 2021-05-22          2250.00             2009.45
         2 2021-07-05          1200.00             1200.00

Summary Statistics:
----------------------------------------------------------
Aggregate Advance Balance:                         3209.45
Interest Payable Balance:                            40.48
Total Interest Paid:                                  9.45
Balance Applicable to Future Advances:                0.00

$ python cli.py balances 2021-07-30
Advances:
----------------------------------------------------------
Identifier       Date      Initial Amt     Current Balance
         1 2021-05-22          2250.00                0.00
         2 2021-07-05          1200.00                0.00

Summary Statistics:
----------------------------------------------------------
Aggregate Advance Balance:                            0.00
Interest Payable Balance:                             0.00
Total Interest Paid:                                 57.79
Balance Applicable to Future Advances:              742.21

$ python cli.py balances 2021-08-04
Advances:
----------------------------------------------------------
Identifier       Date      Initial Amt     Current Balance
         1 2021-05-22          2250.00                0.00
         2 2021-07-05          1200.00                0.00
         3 2021-08-04          1500.00              757.79

Summary Statistics:
----------------------------------------------------------
Aggregate Advance Balance:                          757.79
Interest Payable Balance:                             0.27
Total Interest Paid:                                 57.79
Balance Applicable to Future Advances:                0.00
```
See below for a **full example**:

```
$ python cli.py create-db
Initialized database at db.sqlite3

$ python cli.py load tests/test2.csv
Loaded 5 events from tests/test2.csv

$ python cli.py balances
Advances:
----------------------------------------------------------
Identifier       Date      Initial Amt     Current Balance
         1 2021-05-22          2250.00                0.00
         2 2021-07-05          1200.00                0.00
         3 2021-10-01           100.00                0.00

Summary Statistics:
----------------------------------------------------------
Aggregate Advance Balance:                            0.00
Interest Payable Balance:                             0.00
Total Interest Paid:                                 57.79
Balance Applicable to Future Advances:              642.21

$ python cli.py balances 2021-07-25
Advances:
----------------------------------------------------------
Identifier       Date      Initial Amt     Current Balance
         1 2021-05-22          2250.00             2009.45
         2 2021-07-05          1200.00             1200.00

Summary Statistics:
----------------------------------------------------------
Aggregate Advance Balance:                         3209.45
Interest Payable Balance:                            46.10
Total Interest Paid:                                  9.45
Balance Applicable to Future Advances:                0.00

$ python cli.py drop-db
Deleted SQLite database at db.sqlite3
```


## Overall Criteria

Your work will be judged on:

- The structure and quality of your code
- The ability for your code to achieve the functionality described above
- The accuracy of your code against the unit test cases provided

To be respectful of your time, you will not receive additional credit for implementing additional functionality not 
provided in the design requirements above.

## Submission

You can provide your codebase in the form of a GitHub or GitLab repo and email the link to john.turner@getampla.com and 
jie.zhou@getampla.com 
